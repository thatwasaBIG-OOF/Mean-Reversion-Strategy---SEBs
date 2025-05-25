"""
Trading Bot CLI script for tsxapipy.

Implements a basic SMA crossover trading strategy, demonstrates order placement,
active order tracking, position management, and real-time event handling
from both UserHubStream and DataStream.
"""
# pylint: disable=invalid-name  # Allow filename for example script
# Main function and Bot class are inherently complex, disabling some checks for them.
# pylint: disable=too-many-lines
import sys
import os

# ---- sys.path modification ----
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
# ---- End sys.path modification ----

import logging
import time
import argparse
from collections import deque, OrderedDict
from typing import Deque, Optional, Any, Dict, List
from datetime import datetime, timedelta

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.api import APIClient, APIError
from tsxapipy.real_time import UserHubStream, DataStream, StreamConnectionState # Added StreamConnectionState
from tsxapipy.trading.indicators import simple_moving_average
from tsxapipy.trading.logic import decide_trade
from tsxapipy.trading.order_handler import (
    OrderPlacer,
    ORDER_STATUS_WORKING, ORDER_STATUS_FILLED, ORDER_STATUS_PARTIALLY_FILLED,
    ORDER_STATUS_CANCELLED, ORDER_STATUS_REJECTED, ORDER_STATUS_PENDING_NEW,
    ORDER_STATUS_EXPIRED, ORDER_STATUS_UNKNOWN, ORDER_STATUS_TO_STRING_MAP,
    ORDER_SIDES_MAP, ORDER_TYPES_MAP
)
from tsxapipy.config import (
    CONTRACT_ID as DEFAULT_CONFIG_CONTRACT_ID,
    ACCOUNT_ID_TO_WATCH as DEFAULT_CONFIG_ACCOUNT_ID, # This is Optional[int]
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
)
from tsxapipy.common.time_utils import UTC_TZ
from tsxapipy.api.exceptions import ConfigurationError, LibraryError, APIResponseParsingError # Added
from tsxapipy import api_schemas # For type hinting if needed, though OrderPlacer abstracts most

logger = logging.getLogger(__name__)

# --- Constants for Bot Behavior ---
ORDER_POLL_INTERVAL_SECONDS = 30
STALE_ORDER_THRESHOLD_SECONDS = 300

# pylint: disable=too-few-public-methods
class Position:
    """Represents the bot's current position in a contract."""
    def __init__(self, contract_id: str):
        self.contract_id: str = contract_id
        self.size: int = 0
        self.average_entry_price: Optional[float] = None
        self.entry_order_ids: List[int] = []
        self.realized_pnl: float = 0.0
        logger.info("Position object initialized for %s", contract_id)

    # pylint: disable=too-many-arguments, too-many-branches, too-many-statements
    def update_on_fill(self,
                       fill_qty_this_event: int,
                       fill_price_this_event: float,
                       order_side_code: int, # 0 for BUY, 1 for SELL
                       order_id: Optional[int] = None):
        """Updates the position based on a specific fill event."""
        if not (isinstance(fill_qty_this_event, int) and fill_qty_this_event > 0 and
                isinstance(fill_price_this_event, (float, int)) and
                isinstance(order_side_code, int) and order_side_code in [0, 1]):
            log_msg = ("Position.update_on_fill: Invalid arguments. Qty=%s, "
                       "Price=%s, SideCode=%s. Cannot update.",
                       fill_qty_this_event, fill_price_this_event, order_side_code)
            logger.warning(*log_msg)
            return

        log_fill_info = ("Position: Processing specific fill for Order ID %s: "
                         "SideCode=%d, Qty=%d, Price=%.2f",
                         order_id or 'N/A', order_side_code,
                         fill_qty_this_event, fill_price_this_event)
        logger.info(*log_fill_info)

        if self.size == 0: # Entering a new position
            self.average_entry_price = float(fill_price_this_event)
            self.size = fill_qty_this_event if order_side_code == 0 \
                                            else -fill_qty_this_event
            if order_id and order_id not in self.entry_order_ids:
                self.entry_order_ids.append(order_id)
        else: # Modifying an existing position
            original_size = self.size
            original_avg_price = self.average_entry_price or 0.0
            # current_total_value = original_size * original_avg_price # Not used

            if (original_size > 0 and order_side_code == 0) or \
               (original_size < 0 and order_side_code == 1): # Adding to existing position
                current_total_value = original_size * original_avg_price
                additional_value = (fill_qty_this_event * float(fill_price_this_event)) \
                                   if order_side_code == 0 \
                                   else (-fill_qty_this_event * float(fill_price_this_event))
                
                self.size += fill_qty_this_event if order_side_code == 0 \
                                                 else -fill_qty_this_event
                if self.size != 0:
                    self.average_entry_price = (current_total_value + additional_value) / self.size
                else: # Should not happen if adding to existing, but defensive
                    self.average_entry_price = None
                
                if order_id and order_id not in self.entry_order_ids:
                    self.entry_order_ids.append(order_id)
            else: # Reducing or flipping position
                pnl_this_fill = 0.0
                qty_closed = min(abs(original_size), fill_qty_this_event)
                
                if original_size > 0: # Was long, this fill is a SELL
                    pnl_this_fill = (float(fill_price_this_event) - original_avg_price) \
                                    * qty_closed
                    self.size -= fill_qty_this_event
                elif original_size < 0: # Was short, this fill is a BUY
                    pnl_this_fill = (original_avg_price - float(fill_price_this_event)) \
                                    * qty_closed
                    self.size += fill_qty_this_event
                
                self.realized_pnl += pnl_this_fill
                logger.info("Realized PnL from this fill: %.2f. Total Realized PnL: %.2f",
                            pnl_this_fill, self.realized_pnl)
                
                if self.size == 0: # Position fully closed
                    self.average_entry_price = None
                    self.entry_order_ids = []
                elif (original_size > 0 and self.size < 0) or \
                     (original_size < 0 and self.size > 0): # Position flipped
                    self.average_entry_price = float(fill_price_this_event) # New entry price is this fill's price
                    self.entry_order_ids = [order_id] if order_id else [] # Start new list of entry orders
                # If position reduced but not closed/flipped, average_entry_price remains unchanged for remaining lots

        avg_px_display = "%.2f" % self.average_entry_price \
            if self.average_entry_price is not None else 'N/A'
        logger.info("Position Updated: Contract=%s, Size=%d, AvgEntryPx=%s",
                    self.contract_id, self.size, avg_px_display)

# pylint: disable=too-many-instance-attributes
class TradingBot:
    """
    Trading bot implementing an SMA crossover strategy.
    Manages market data, user data streams, order placement, and position tracking.
    """
    # pylint: disable=too-many-arguments
    def __init__(self,
                 contract_id: str,
                 sma_period: int,
                 max_price_history: int,
                 api_client: APIClient,
                 account_id: int):
        self.contract_id: str = contract_id
        self.sma_period: int = sma_period
        self.price_history: Deque[float] = deque(maxlen=max_price_history)
        self.api_client: APIClient = api_client
        self.account_id: int = account_id
        self.order_placer: OrderPlacer = OrderPlacer(api_client, account_id,
                                                     default_contract_id=contract_id)
        self.user_hub_stream: Optional[UserHubStream] = None
        self.market_data_stream: Optional[DataStream] = None
        self.active_orders: OrderedDict[int, Dict[str, Any]] = OrderedDict() # Stores raw dicts from stream/poll
        self.position: Position = Position(contract_id)
        self.last_signal_decision: Optional[str] = None
        self.entry_order_id: Optional[int] = None
        self.is_exiting_position: bool = False
        logger.info("TradingBot initialized for Contract: %s, Account: %s, SMA(%d)",
                    self.contract_id, self.account_id, sma_period)

    def _update_active_order(self, order_data: Dict[str, Any]):
        """Helper to update or add an order to the active_orders tracking dict."""
        order_id = order_data.get("id")
        if not isinstance(order_id, int):
            logger.warning("Bot: _update_active_order with invalid order_id in data: %s",
                           order_data)
            return

        if order_id not in self.active_orders:
            # Only track orders for this bot's account and contract
            if order_data.get("accountId") == self.account_id and \
               order_data.get("contractId") == self.contract_id:
                self.active_orders[order_id] = {} # Initialize if new
                logger.info("Bot: Now tracking new order %d (Type: %s, Side: %s, Size: %s).",
                            order_id, 
                            ORDER_TYPES_MAP.get(str(order_data.get('type',"")), order_data.get('type')), # Try to map for log
                            ORDER_SIDES_MAP.get(str(order_data.get('side',"")), order_data.get('side')),
                            order_data.get('size'))
            else:
                # logger.debug(f"Bot: Ignoring order update for different account/contract: {order_id}")
                return # Not for this bot

        # Update fields, ensure they exist in order_data or default gracefully
        updated_fields = {
            "id": order_id, 
            "status": order_data.get("status"),
            "side": order_data.get("side"), 
            "type": order_data.get("type"),
            "size": order_data.get("size"),
            "cumQuantity": order_data.get("cumQuantity", 0),
            "avgPx": order_data.get("avgPx"),
            "limitPrice": order_data.get("limitPrice"),
            "stopPrice": order_data.get("stopPrice"),
            "contractId": order_data.get("contractId"),
            "accountId": order_data.get("accountId"),
            "creationTimestamp": order_data.get("creationTimestamp"), # Keep as string from API/stream
            "updateTimestamp": order_data.get("updateTimestamp"),   # Keep as string
            "lastUpdateTimeBot": datetime.now(UTC_TZ) # Add bot's own update time
        }
        # Calculate leavesQuantity if not directly provided
        if "leavesQuantity" in order_data:
            updated_fields["leavesQuantity"] = order_data.get("leavesQuantity")
        elif updated_fields.get("size") is not None and updated_fields.get("cumQuantity") is not None:
            try:
                updated_fields["leavesQuantity"] = int(updated_fields["size"]) - int(updated_fields["cumQuantity"])
            except (ValueError, TypeError):
                logger.warning(f"Bot: Could not calculate leavesQuantity for order {order_id}")
                updated_fields["leavesQuantity"] = None


        self.active_orders[order_id].update(updated_fields)
        status_str = ORDER_STATUS_TO_STRING_MAP.get(updated_fields['status'], f"UNKNOWN({updated_fields['status']})")
        logger.debug("Bot: Updated active_orders cache for Order ID %d: Status=%s, CumQty=%s",
                     order_id, status_str, updated_fields.get('cumQuantity'))

    # pylint: disable=too-many-branches, too-many-statements
    def handle_realtime_order_update(self, order_data: Dict[str, Any]):
        """Handles order updates received from the UserHubStream."""
        order_id = order_data.get("id")
        if not isinstance(order_id, int) or \
           order_data.get("accountId") != self.account_id or \
           order_data.get("contractId") != self.contract_id:
            # logger.debug(f"Bot: Ignoring order update not matching bot's account/contract. Event OrderID: {order_id}")
            return

        status_code = order_data.get("status")
        status_str = ORDER_STATUS_TO_STRING_MAP.get(status_code, f"UNKNOWN_STATUS({status_code})")
        
        log_msg = ("[ORDER UPDATE EVENT] ID: %s, Status: %s, CumQty: %s, LeavesQty: %s, AvgPx: %s",
                   order_id, status_str,
                   order_data.get('cumQuantity', 'N/A'), 
                   order_data.get('leavesQuantity', 'N/A'),
                   order_data.get('avgPx', 'N/A'))
        logger.info(*log_msg)
        self._update_active_order(order_data) # Update internal cache

        is_entry_order = (order_id == self.entry_order_id)
        is_currently_tracked_exit_order = False
        if self.is_exiting_position:
            # Check if this order_id is one of the orders placed to exit
            # This simple bot only places one exit order at a time, so check active_orders.
             if order_id in self.active_orders: # If it's still in active_orders, it's likely our exit attempt
                 is_currently_tracked_exit_order = True


        if status_code == ORDER_STATUS_FILLED:
            logger.info("Bot: Order %s FULLY FILLED via stream! Position reconciliation will occur via UserTrade event or polling.", order_id)
            if is_entry_order:
                self.entry_order_id = None # Clear entry order ID
            if is_currently_tracked_exit_order and self.position.size == 0: # If exit order filled and position is now flat
                self.is_exiting_position = False
            if order_id in self.active_orders: # Remove from active tracking
                del self.active_orders[order_id]

        elif status_code == ORDER_STATUS_PARTIALLY_FILLED:
            logger.info("Bot: Order %s PARTIALLY FILLED via stream. Awaiting UserTrade for position update.", order_id)
            # Keep in active_orders

        elif status_code in [ORDER_STATUS_CANCELLED, ORDER_STATUS_REJECTED, ORDER_STATUS_EXPIRED]:
            logger.info("Bot: Order %s (%s) is now terminal (Cancelled/Rejected/Expired) via stream.", order_id, status_str)
            if is_entry_order:
                self.entry_order_id = None
                self.last_signal_decision = None # Allow new signal
            if is_currently_tracked_exit_order:
                self.is_exiting_position = False # Stop considering ourselves as "actively exiting"
                logger.warning("Bot: Exit order %s %s! Current Position Size: %d. Bot may need to re-evaluate exit.",
                               order_id, status_str, self.position.size)
            if order_id in self.active_orders: # Remove from active tracking
                del self.active_orders[order_id]
        
        elif status_code in [ORDER_STATUS_WORKING, ORDER_STATUS_PENDING_NEW, ORDER_STATUS_NEW]:
            logger.debug("Bot: Order %s is active/pending: %s.", order_id, status_str)
        
        else: # ORDER_STATUS_UNKNOWN or any other unhandled code
            logger.warning("Bot: Order %s has an unhandled status from stream: %s (Code: %s).",
                           order_id, status_str, status_code)

        # Log bot's state after processing the update
        log_state_summary = (
            "Bot State After Order Update: Position Size=%d, Current Entry OrderID=%s, "
            "Is Exiting Flag=%s, Number of Active Orders Tracked=%d",
            self.position.size, self.entry_order_id, self.is_exiting_position, len(self.active_orders)
        )
        logger.debug(*log_state_summary)


    def handle_account_update(self, acc_data: Dict[str, Any]):
        """Handles account updates from UserHubStream."""
        if acc_data.get("id") == self.account_id:
            logger.info("Bot: Received AccountUpdate for watched account %s: Balance=%s",
                        self.account_id, acc_data.get('balance'))
        else:
            logger.debug("Bot: Received AccountUpdate for other account: %s",
                         acc_data.get('id'))

    def handle_position_update(self, pos_data: Dict[str, Any]):
        """Handles position updates from UserHubStream (viewed as less authoritative than individual trades)."""
        if pos_data.get("accountId") == self.account_id and \
           pos_data.get("contractId") == self.contract_id:
            log_msg = ("Bot: Received PositionUpdate from stream for %s: Size=%s, "
                       "AvgPx=%s (Note: Bot uses UserTrade events for primary position tracking).", 
                       self.contract_id, pos_data.get('size'),
                       pos_data.get('averagePrice'))
            logger.info(*log_msg)
            # Could add logic here to compare with self.position and log discrepancies,
            # but primary updates come from UserTrade or reconciliation.

    # pylint: disable=too-many-branches
    def handle_user_trade_update(self, trade_data: Dict[str, Any]):
        """Handles user trade execution updates from UserHubStream (primary source for position changes)."""
        if trade_data.get("accountId") == self.account_id and \
           trade_data.get("contractId") == self.contract_id:
            order_id_from_trade = trade_data.get("orderId")
            trade_side_code = trade_data.get("side") # API: 0=Buy, 1=Sell
            trade_qty = trade_data.get("size")
            trade_price = trade_data.get("price")

            log_msg = ("[USER TRADE EXECUTION EVENT] OrderID: %s, SideCode: %s, Qty: %s, "
                       "Price: %s, PnL (from API trade event): %s", 
                       order_id_from_trade, trade_side_code, 
                       trade_qty, trade_price,
                       trade_data.get('profitAndLoss'))
            logger.info(*log_msg)

            if trade_qty is not None and trade_price is not None and trade_side_code is not None:
                try:
                    self.position.update_on_fill(
                        fill_qty_this_event=int(trade_qty),
                        fill_price_this_event=float(trade_price),
                        order_side_code=int(trade_side_code),
                        order_id=order_id_from_trade # Pass order ID for tracking entry orders
                    )
                    # If an exit order was being tracked and this trade makes position flat
                    if self.is_exiting_position and self.position.size == 0:
                        logger.info("Bot: Position is now flat after UserTrade. Clearing 'is_exiting_position' flag.")
                        self.is_exiting_position = False
                except (ValueError, TypeError) as e_fill:
                    logger.error(f"Bot: Error converting trade data for position update: {e_fill}. Trade data: {trade_data}")
            else:
                logger.warning("Bot: Incomplete data in UserTrade for OrderID %s (Qty, Price, or Side missing). "
                               "Cannot update bot's position accurately from this event.",
                               order_id_from_trade)
        else:
            # Log if it's for a different account/contract but not too verbosely
            # logger.debug("Bot: Received UserTradeUpdate for other account/contract. OrderID: %s",
            #              trade_data.get('orderId'))
            pass


    def handle_market_quote_payload(self, quote_data: Dict[str, Any]):
        """Handles market quote payloads from DataStream (not used by this bot's SMA logic)."""
        logger.debug("Bot: Received MarketQuote (Side: %s @ %s / %s @ %s)",
                     quote_data.get('bs'), quote_data.get('bp'),
                     quote_data.get('ap'), quote_data.get('as'))

    def handle_market_depth_payload(self, depth_data: Any): # pylint: disable=unused-argument
        """Handles market depth payloads from DataStream (not used by this bot)."""
        logger.debug("Bot: Received MarketDepth (not used by bot)")


    def _can_place_new_entry_order(self) -> bool:
        """Checks if conditions allow placing a new entry order."""
        if self.entry_order_id and self.entry_order_id in self.active_orders:
            # If the entry order is still active (e.g., PENDING_NEW, WORKING)
            # This check uses self.active_orders which is updated by stream and polling.
            order_status = self.active_orders[self.entry_order_id].get("status")
            status_str = ORDER_STATUS_TO_STRING_MAP.get(order_status, f"UNKNOWN({order_status})")
            logger.debug(f"Bot: Cannot place new entry: existing entry order {self.entry_order_id} is active (Status: {status_str}).")
            return False
        if self.is_exiting_position:
            logger.debug("Bot: Cannot place new entry: currently in the process of exiting a position.")
            return False
        return True

    def _attempt_entry(self, decision: str):
        """Attempts to enter a position based on the trading decision (BUY/SELL)."""
        if not self._can_place_new_entry_order():
            return # Conditions not met (e.g., existing entry order, or exiting position)

        if self.position.size != 0: # Should already be flat if _can_place_new_entry_order passed unless is_exiting_position was false
            logger.info("Bot: Signal to %s, but current position size is %d. Attempting to flatten first.",
                        decision.upper(), self.position.size)
            self._attempt_exit_position() # This will set is_exiting_position
            return # Wait for exit to complete before considering new entry

        logger.info("Bot: Attempting to ENTER %s position for %s, Market Order, Size: 1.",
                    decision.upper(), self.contract_id)
        
        order_side_for_api = decision.upper() # "BUY" or "SELL" directly used by OrderPlacer
        
        # OrderPlacer.place_market_order handles mapping to side code and creating Pydantic model
        order_id = self.order_placer.place_market_order(side=order_side_for_api, size=1)

        if order_id:
            logger.info("Bot: Market %s order to ENTER submitted. API Order ID: %s. Awaiting stream confirmation.",
                        decision.upper(), order_id)
            self.entry_order_id = order_id # Track this as the current entry attempt
            # Add to active_orders immediately with PENDING_NEW status for internal tracking
            # This will be updated by stream or polling.
            self._update_active_order({
                "id": order_id, 
                "status": ORDER_STATUS_PENDING_NEW, # Initial assumption
                "side": ORDER_SIDES_MAP[order_side_for_api], 
                "type": ORDER_TYPES_MAP["MARKET"],
                "size": 1, 
                "contractId": self.contract_id,
                "accountId": self.account_id,
                "creationTimestamp": datetime.now(UTC_TZ).isoformat(), # Approximate
                "lastUpdateTimeBot": datetime.now(UTC_TZ)
            })
            self.last_signal_decision = decision # Store the signal that led to this entry
        else:
            logger.error("Bot: Failed to place market %s order to enter (OrderPlacer returned None).",
                         decision.upper())
            self.last_signal_decision = None # Reset last signal if order placement failed

    def _attempt_exit_position(self):
        """Attempts to exit the current position by placing a counter market order."""
        if self.position.size == 0:
            logger.debug("Bot: No position to exit.")
            return
        
        # Check if there's already an active exit order
        # This is a simple check; more complex logic might track specific exit order IDs.
        if self.is_exiting_position and \
           any(o.get("status") in [ORDER_STATUS_PENDING_NEW, ORDER_STATUS_WORKING, ORDER_STATUS_PARTIALLY_FILLED]
               for o in self.active_orders.values() if o.get("id") != self.entry_order_id): # Exclude entry orders
            logger.info("Bot: Already attempting to exit position with an active order.")
            return

        exit_side_str = "SELL" if self.position.size > 0 else "BUY"
        exit_size = abs(self.position.size)
        logger.info("Bot: Attempting to EXIT position of %d for %s with MARKET %s order, Size: %d.",
                    self.position.size, self.contract_id, exit_side_str, exit_size)

        order_id = self.order_placer.place_market_order(side=exit_side_str.upper(), size=exit_size)
        
        if order_id:
            self.is_exiting_position = True # Set flag indicating an exit is in progress
            logger.info("Bot: Market %s order to EXIT position submitted. API Order ID: %s.",
                        exit_side_str, order_id)
            # Add to active_orders for tracking
            self._update_active_order({
                "id": order_id,
                "status": ORDER_STATUS_PENDING_NEW,
                "side": ORDER_SIDES_MAP[exit_side_str.upper()],
                "type": ORDER_TYPES_MAP["MARKET"],
                "size": exit_size,
                "contractId": self.contract_id,
                "accountId": self.account_id,
                "creationTimestamp": datetime.now(UTC_TZ).isoformat(),
                "lastUpdateTimeBot": datetime.now(UTC_TZ)
            })
        else:
            logger.error("Bot: Failed to place market %s order to exit position (OrderPlacer returned None).",
                         exit_side_str)
            self.is_exiting_position = False # Reset flag if placement failed

    # pylint: disable=too-many-branches
    def process_market_data_tick(self, trade_data_item: Any):
        """Processes a single market data tick (trade) to make trading decisions."""
        if not isinstance(trade_data_item, dict): return # Expecting a dict from DataStream trade callback
        
        price_value = trade_data_item.get('price') # 'price' from DataStream trade
        if price_value is None: price_value = trade_data_item.get('p') # Fallback for some quote structures
        
        if price_value is None:
            # logger.debug("Bot: Market data tick missing price field. Data: %s", trade_data_item)
            return
        try:
            price = float(price_value)
        except (ValueError, TypeError):
            logger.warning("Bot: Invalid price data in market tick: '%s'. Cannot convert to float.", price_value)
            return

        self.price_history.append(price)
        if len(self.price_history) < self.sma_period:
            logger.debug("Bot: Not enough price history (%d points) for SMA period %d. Current price: %.2f",
                         len(self.price_history), self.sma_period, price)
            return

        ma_val = simple_moving_average(list(self.price_history), self.sma_period)
        if ma_val is None:
            logger.warning("Bot: Could not calculate SMA from price history (length %d).", len(self.price_history))
            return

        current_signal = decide_trade(price, ma_val) # Returns "BUY", "SELL", or None
        
        log_tick_info = ("Bot Tick Eval: Price=%.2f, MA(%d)=%.2f, Signal=%s | Current State: PosSize=%d, "
                         "EntryOrderID=%s, IsExiting=%s, LastSignalDecision=%s",
                         price, self.sma_period, ma_val, current_signal or "NONE",
                         self.position.size, self.entry_order_id, self.is_exiting_position,
                         self.last_signal_decision)
        logger.info(*log_tick_info)

        # If currently trying to exit, don't make new entry decisions based on this tick
        if self.is_exiting_position:
            # Check if any active order is an exit order and still working
            is_active_exit_order_present = any(
                o.get("status") in [ORDER_STATUS_PENDING_NEW, ORDER_STATUS_WORKING, ORDER_STATUS_PARTIALLY_FILLED] and
                o.get("id") != self.entry_order_id # Ensure it's not an old entry order
                for o in self.active_orders.values()
            )
            if is_active_exit_order_present:
                logger.debug("Bot: Currently exiting position with an active order. No new trade decisions on this tick.")
                return
            else: # No active exit order found, but flag is true - maybe it filled/cancelled?
                logger.warning("Bot: 'is_exiting_position' is true, but no active exit order found. Re-evaluating state.")
                # Position reconciliation or stream updates should ideally clear this flag.
                # For safety, if position is now flat, clear the flag.
                if self.position.size == 0:
                    self.is_exiting_position = False


        # Trading Logic:
        if self.position.size == 0: # Currently flat
            if self._can_place_new_entry_order(): # Check if safe to place new entry
                if current_signal == "BUY" and self.last_signal_decision != "BUY":
                    self._attempt_entry("BUY")
                elif current_signal == "SELL" and self.last_signal_decision != "SELL":
                    self._attempt_entry("SELL")
                # If no signal or same as last, do nothing.
            # else: Cannot place new entry order right now (e.g., existing entry order active)
        
        elif self.position.size > 0: # Currently LONG
            if current_signal == "SELL": # Signal to exit LONG
                logger.info("Bot: SELL signal received while LONG. Attempting to exit position.")
                self._attempt_exit_position()
            # If BUY signal while already LONG, hold.
        
        elif self.position.size < 0: # Currently SHORT
            if current_signal == "BUY": # Signal to exit SHORT
                logger.info("Bot: BUY signal received while SHORT. Attempting to exit position.")
                self._attempt_exit_position()
            # If SELL signal while already SHORT, hold.


    def handle_market_data_payload(self, market_payload: Any):
        """Handles the overall market data payload from DataStream (usually a list of trades)."""
        if isinstance(market_payload, list): # DataStream may send a list of trades
            for item in market_payload:
                self.process_market_data_tick(item)
        elif isinstance(market_payload, dict): # Or a single trade object
            self.process_market_data_tick(market_payload)
        else:
            logger.warning("Bot: Received unexpected market_payload type: %s", type(market_payload))

    # pylint: disable=too-many-branches
    def poll_active_orders_status(self, search_window_minutes: int = 30):
        """Periodically polls the status of tracked active orders via API."""
        if not self.active_orders:
            logger.debug("Bot: No active orders to poll.")
            return

        logger.info("Bot: Polling status for %d active order(s) via API...",
                    len(self.active_orders))
        
        # Iterate over a copy of keys because handle_realtime_order_update might modify self.active_orders
        for order_id in list(self.active_orders.keys()): 
            if order_id not in self.active_orders: # Check if removed during iteration by a stream update
                logger.debug(f"Bot: Order {order_id} removed from active tracking (likely by stream update) before polling.")
                continue

            order_snapshot_in_cache = self.active_orders[order_id]
            # Fallback to a very old time if 'creationTimestamp' is missing or invalid for PENDING_NEW check
            creation_ts_str = order_snapshot_in_cache.get("creationTimestamp")
            try:
                order_creation_time = datetime.fromisoformat(creation_ts_str.replace("Z", "+00:00")) if creation_ts_str else datetime.now(UTC_TZ) - timedelta(days=1)
            except (ValueError, AttributeError):
                order_creation_time = datetime.now(UTC_TZ) - timedelta(days=1)


            last_bot_update_time = order_snapshot_in_cache.get("lastUpdateTimeBot", datetime.now(UTC_TZ) - timedelta(days=1))
            
            force_poll_this_order = False
            # Force poll if PENDING_NEW for too long (e.g., >30s)
            if order_snapshot_in_cache.get("status") == ORDER_STATUS_PENDING_NEW and \
               (datetime.now(UTC_TZ) - order_creation_time) > timedelta(seconds=30) :
                logger.warning("Bot: Order %s has been PENDING_NEW for >30s. Forcing API poll.", order_id)
                force_poll_this_order = True
            # Force poll if WORKING but not updated recently by stream (stale check)
            elif order_snapshot_in_cache.get("status") == ORDER_STATUS_WORKING and \
                 (datetime.now(UTC_TZ) - last_bot_update_time) > timedelta(seconds=STALE_ORDER_THRESHOLD_SECONDS):
                status_str = ORDER_STATUS_TO_STRING_MAP.get(order_snapshot_in_cache.get('status'), "UNKNOWN")
                logger.warning("Bot: Order %s (Status: %s) in cache seems stale (last bot update: %s). Forcing API poll.",
                             order_id, status_str, last_bot_update_time.isoformat())
                force_poll_this_order = True

            # Regular poll interval check if not forced
            if not force_poll_this_order and \
               (datetime.now(UTC_TZ) - last_bot_update_time) < timedelta(seconds=ORDER_POLL_INTERVAL_SECONDS / 2.0): # Reduce redundant polls if stream is active
                # logger.debug(f"Bot: Order {order_id} updated recently by bot/stream. Skipping this poll cycle.")
                continue

            try:
                logger.debug("Bot: Polling API for details of order %s...", order_id)
                # OrderPlacer.get_order_details now returns Optional[api_schemas.OrderDetails]
                live_details_model: Optional[api_schemas.OrderDetails] = self.order_placer.get_order_details(
                    order_id_to_find=order_id, 
                    search_window_minutes=search_window_minutes
                )
                
                if live_details_model:
                    # Convert Pydantic model to dict for handle_realtime_order_update and comparison
                    live_details_dict = live_details_model.model_dump(by_alias=False) # Use Python field names

                    # Compare key fields to see if an update is needed
                    changed_status = order_snapshot_in_cache.get("status") != live_details_dict.get("status")
                    changed_cum_qty = order_snapshot_in_cache.get("cumQuantity") != live_details_dict.get("cumQuantity")
                    
                    if changed_status or changed_cum_qty:
                        log_poll_update_msg = ("[POLL API UPDATE] Order ID %s data changed. Cache Status: %s -> API Status: %s. Cache CumQty: %s -> API CumQty: %s",
                                             order_id, 
                                             ORDER_STATUS_TO_STRING_MAP.get(order_snapshot_in_cache.get('status')),
                                             ORDER_STATUS_TO_STRING_MAP.get(live_details_dict.get('status')),
                                             order_snapshot_in_cache.get('cumQuantity'),
                                             live_details_dict.get('cumQuantity'))
                        logger.info(*log_poll_update_msg)
                        self.handle_realtime_order_update(live_details_dict) # Process the new details
                    elif order_id in self.active_orders: # If no change, just update the bot's last seen time for this order
                        self.active_orders[order_id]["lastUpdateTimeBot"] = datetime.now(UTC_TZ)
                        logger.debug(f"Bot: Polled order {order_id}, no significant change from cached state.")
                
                elif order_snapshot_in_cache.get("status") in [ORDER_STATUS_PENDING_NEW, ORDER_STATUS_WORKING, ORDER_STATUS_PARTIALLY_FILLED]:
                    # If API says order not found, but cache thinks it's active, it might be terminal (Cancelled/Filled/Rejected and old)
                    status_str_cache = ORDER_STATUS_TO_STRING_MAP.get(order_snapshot_in_cache.get('status'), "UNKNOWN")
                    logger.warning("[POLL NOT FOUND] Active order %s (Cached Status: %s) not found via API search. "
                                   "Assuming it became terminal and was missed by stream. Marking as UNKNOWN.",
                                   order_id, status_str_cache)
                    # Simulate an update to UNKNOWN to trigger removal from active_orders by handle_realtime_order_update
                    self.handle_realtime_order_update({
                        "id": order_id, 
                        "status": ORDER_STATUS_UNKNOWN, # This will trigger removal logic
                        "accountId": self.account_id, # Needed for _update_active_order filter
                        "contractId": self.contract_id # Needed for _update_active_order filter
                    })
            except APIResponseParsingError as e_parse:
                 logger.error(f"Bot: Error parsing API response while polling order {order_id}: {e_parse}")
            except APIError as e_api_poll:
                logger.error("Bot: APIError polling status for order %s: %s", order_id, e_api_poll)
            except Exception as e_poll_generic: # pylint: disable=broad-except
                logger.error("Bot: Unexpected error polling status for order %s: %s",
                             order_id, e_poll_generic, exc_info=True)


    # pylint: disable=too-many-branches
    def reconcile_position(self):
        """Reconciles the bot's tracked position with the API's open positions."""
        log_msg = ("Bot: Attempting to reconcile position for Account %s, Contract %s...",
                   self.account_id, self.contract_id)
        logger.info(*log_msg)
        try:
            # APIClient.search_open_positions returns List[api_schemas.Position]
            open_positions_api: List[api_schemas.Position] = self.api_client.search_open_positions(
                account_id=self.account_id
            )
            api_pos_model_for_contract: Optional[api_schemas.Position] = None
            for pos_model in open_positions_api:
                if pos_model.contract_id == self.contract_id:
                    api_pos_model_for_contract = pos_model
                    break

            if api_pos_model_for_contract:
                api_size = api_pos_model_for_contract.size if api_pos_model_for_contract.size is not None else 0
                api_avg_px = api_pos_model_for_contract.average_price # Already float or None

                logger.info("Bot: API Position for %s: Size=%s, AvgPrice=%s",
                            self.contract_id, api_size, 
                            f"{api_avg_px:.4f}" if api_avg_px is not None else "N/A")

                bot_avg_px = self.position.average_entry_price
                
                # Check for mismatch
                mismatch = False
                if api_size != self.position.size:
                    mismatch = True
                elif api_size != 0: # Only compare price if there's a position
                    if (api_avg_px is None and bot_avg_px is not None) or \
                       (api_avg_px is not None and bot_avg_px is None) or \
                       (api_avg_px is not None and bot_avg_px is not None and \
                        abs(api_avg_px - bot_avg_px) > 0.0001): # Tolerance for float comparison
                        mismatch = True
                
                if mismatch:
                    log_mismatch_msg = ("Bot: POSITION RECONCILIATION MISMATCH for %s: "
                                    "API Size=%s (AvgPx=%s) vs Bot Size=%s (AvgPx=%s). "
                                    "UPDATING BOT'S INTERNAL POSITION STATE.", 
                                    self.contract_id, api_size, 
                                    f"{api_avg_px:.4f}" if api_avg_px is not None else "N/A", 
                                    self.position.size, 
                                    f"{bot_avg_px:.4f}" if bot_avg_px is not None else "N/A")
                    logger.warning(*log_mismatch_msg)
                    
                    self.position.size = api_size
                    self.position.average_entry_price = api_avg_px
                    
                    if self.position.size == 0: # If API says flat
                        self.position.entry_order_ids = [] # Clear entry orders
                        self.entry_order_id = None # Clear current entry attempt
                        self.is_exiting_position = False # Not exiting if flat
                        self.last_signal_decision = None # Allow new signal
                    elif not self.position.entry_order_ids and self.entry_order_id is None:
                        # Position exists on API, but bot has no record of how it entered.
                        # This is a state divergence. Bot might adopt the position but won't know "why".
                        logger.warning("Bot: Position exists on API (Size: %d) for %s, "
                                       "but bot has no tracked entry order IDs. Adopted API state.",
                                       self.position.size, self.contract_id)
                        # Cannot reconstruct entry_order_ids from this alone.
                else:
                    logger.info("Bot: Position for %s matches API (Size: %d, AvgPx: %s). Reconciliation successful.",
                                self.contract_id, self.position.size,
                                f"{self.position.average_entry_price:.4f}" if self.position.average_entry_price is not None else "N/A")
            
            elif self.position.size != 0: # API shows no position for this contract, but bot thinks it has one
                log_no_api_pos_msg = ("Bot: POSITION RECONCILIATION: API shows NO open "
                                  "position for %s, but bot's internal state is Size=%d. "
                                  "RESETTING BOT'S INTERNAL POSITION TO FLAT.", 
                                  self.contract_id, self.position.size)
                logger.warning(*log_no_api_pos_msg)
                self.position.size = 0
                self.position.average_entry_price = None
                self.position.entry_order_ids = []
                self.is_exiting_position = False
                self.entry_order_id = None
                self.last_signal_decision = None
            
            else: # Both API and bot show no position for this contract
                logger.info("Bot: POSITION RECONCILIATION: Bot and API both show no "
                            "open position for %s. State is consistent.", self.contract_id)

        except APIResponseParsingError as e_parse_pos:
            logger.error("Bot: Error parsing API response during position reconciliation: %s", e_parse_pos)
        except APIError as e_api_pos:
            logger.error("Bot: API Error during position reconciliation: %s", e_api_pos)
        except Exception as e_rec_generic: # pylint: disable=broad-except
            logger.error("Bot: Unexpected error during position reconciliation: %s",
                         e_rec_generic, exc_info=True)


# pylint: disable=too-many-locals, too-many-branches, too-many-statements
def main():
    """Main function to set up and run the trading bot."""
    parser = argparse.ArgumentParser(
        description="Trading bot with SMA crossover strategy."
    )
    parser.add_argument("--contract_id", type=str,
                        default=DEFAULT_CONFIG_CONTRACT_ID,
                        help="Contract ID for trading.")
    parser.add_argument("--account_id", type=int,
                        # Ensure default from config is treated as Optional[int] or handle None from getenv
                        default=int(DEFAULT_CONFIG_ACCOUNT_ID) if DEFAULT_CONFIG_ACCOUNT_ID is not None else 0,
                        help="Account ID for trading. Must be a positive integer.")
    parser.add_argument("--sma_period", type=int, default=5,
                        help="Period for Simple Moving Average.")
    parser.add_argument("--price_history_size", type=int, default=100,
                        help="Max number of prices to keep for MA.")
    parser.add_argument("--poll_interval", type=int,
                        default=ORDER_POLL_INTERVAL_SECONDS,
                        dest="poll_interval_seconds",
                        help="Order status polling interval (seconds).")
    parser.add_argument("--reconcile_interval", type=int, default=300,
                        dest="reconcile_interval_seconds",
                        help="Position reconciliation interval (seconds).")
    parser.add_argument("--no_account_stream", action="store_false",
                        dest="stream_account", default=True,
                        help="Disable UserHubStream subscription to global account updates.")
    parser.add_argument("--no_position_stream", action="store_false",
                        dest="stream_positions", default=True,
                        help="Disable UserHubStream subscription to position updates for the account.")
    parser.add_argument("--no_user_trade_stream", action="store_false",
                        dest="stream_user_trades", default=True,
                        help="Disable UserHubStream subscription to user trade updates for the account.")
    parser.add_argument("--no_quote_stream", action="store_false",
                        dest="stream_quotes", default=True,
                        help="Disable DataStream subscription to quote updates for the contract.")
    parser.add_argument("--no_market_trade_stream", action="store_false",
                        dest="stream_market_trades", default=True,
                        help="Disable DataStream subscription to market trade updates for the contract.")
    parser.add_argument("--no_depth_stream", action="store_false",
                        dest="stream_depth", default=True,
                        help="Disable DataStream subscription to market depth updates for the contract.")
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG level logging.")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True # Ensure this config takes precedence
    )
    if args.debug:
        logger.info("DEBUG logging enabled for all modules.")
        # For very verbose SignalR client logs if needed:
        # logging.getLogger("signalrcore").setLevel(logging.DEBUG) 

    if not args.contract_id:
        logger.error("Contract ID (--contract_id or from .env) is required. Exiting.")
        sys.exit(1)
    if not args.account_id or args.account_id <= 0: # Check after argparse default handling
        logger.error("A valid positive Account ID (--account_id or from .env) is required. Exiting.")
        sys.exit(1)

    logger.info("Starting Trading Bot: Contract=%s, Account=%s, SMA Period=%d",
                args.contract_id, args.account_id, args.sma_period)

    api_client_instance: Optional[APIClient] = None
    bot_instance: Optional[TradingBot] = None
    user_hub_stream_instance: Optional[UserHubStream] = None 
    market_data_stream_instance: Optional[DataStream] = None 

    try:
        logger.info("Authenticating with API...")
        initial_token, token_acquired_time = authenticate()
        api_client_instance = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_time
        )
        logger.info("APIClient initialized successfully.")

        bot_instance = TradingBot(
            contract_id=args.contract_id, sma_period=args.sma_period,
            max_price_history=args.price_history_size,
            api_client=api_client_instance, account_id=args.account_id
        )

        logger.info("Initializing UserHubStream for bot...")
        user_hub_stream_instance = UserHubStream(
            api_client=api_client_instance,
            account_id_to_watch=args.account_id,
            on_order_update=bot_instance.handle_realtime_order_update,
            on_account_update=(bot_instance.handle_account_update if args.stream_account else None),
            on_position_update=(bot_instance.handle_position_update if args.stream_positions else None),
            on_user_trade_update=(bot_instance.handle_user_trade_update if args.stream_user_trades else None),
            subscribe_to_accounts_globally=args.stream_account, # For global "Account" type events
            on_state_change_callback=lambda state_name: logger.info(
                "Bot's UserHubStream state: %s", state_name), # state_name is str
            on_error_callback=lambda err: logger.error(
                "Bot's UserHubStream error: %s", err)
        )
        bot_instance.user_hub_stream = user_hub_stream_instance
        if not user_hub_stream_instance.start():
            status_name = user_hub_stream_instance.connection_status.name if user_hub_stream_instance else "N/A"
            logger.error(f"Failed to start UserHubStream for bot (status: {status_name})! Exiting.")
            sys.exit(1)
        logger.info("UserHubStream start initiated for bot.")


        logger.info("Initializing DataStream for bot...")
        market_data_stream_instance = DataStream(
            api_client=api_client_instance,
            contract_id_to_subscribe=args.contract_id,
            on_trade_callback=(bot_instance.handle_market_data_payload if args.stream_market_trades else None),
            on_quote_callback=(bot_instance.handle_market_quote_payload if args.stream_quotes else None),
            on_depth_callback=(bot_instance.handle_market_depth_payload if args.stream_depth else None),
            on_state_change_callback=lambda state_name: logger.info(
                "Bot's DataStream state: %s", state_name), # state_name is str
            on_error_callback=lambda err: logger.error(
                "Bot's DataStream error: %s", err)
        )
        bot_instance.market_data_stream = market_data_stream_instance
        if not market_data_stream_instance.start():
            status_name = market_data_stream_instance.connection_status.name if market_data_stream_instance else "N/A"
            logger.error(f"Failed to start DataStream for bot (status: {status_name})! Exiting.")
            if user_hub_stream_instance: user_hub_stream_instance.stop(reason_for_stop="DataStream failed to start")
            sys.exit(1)
        logger.info("DataStream start initiated for bot.")

        logger.info("Trading bot running... Press Ctrl+C to stop.")
        last_order_poll_time = time.monotonic()
        last_position_reconcile_time = time.monotonic()
        
        # Calculate token refresh interval, ensure it's reasonable (e.g., at least 60s)
        token_refresh_interval_calc = max(60, (TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES * 60) // 3)
        last_stream_token_check_time = time.monotonic()

        bot_instance.reconcile_position() # Initial reconciliation

        while True:
            current_time_monotonic = time.monotonic()
            if bot_instance: # Check bot_instance exists (it should)
                if current_time_monotonic - last_order_poll_time >= args.poll_interval_seconds:
                    bot_instance.poll_active_orders_status()
                    last_order_poll_time = current_time_monotonic
                
                if current_time_monotonic - last_position_reconcile_time >= args.reconcile_interval_seconds:
                    bot_instance.reconcile_position()
                    last_position_reconcile_time = current_time_monotonic
                
                if current_time_monotonic - last_stream_token_check_time >= token_refresh_interval_calc:
                    logger.debug("Bot: Periodic check to update stream tokens if needed...")
                    try:
                        if api_client_instance: # Should always be true if bot_instance exists
                            latest_api_token = api_client_instance.current_token # Forces re-auth if APIClient token stale
                            
                            if bot_instance.user_hub_stream and \
                               bot_instance.user_hub_stream.connection_status not in [
                                   StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR, StreamConnectionState.STOPPING
                               ]:
                                logger.debug(f"Bot: Updating token for UserHubStream (current state: {bot_instance.user_hub_stream.connection_status.name})")
                                bot_instance.user_hub_stream.update_token(latest_api_token)
                            
                            if bot_instance.market_data_stream and \
                               bot_instance.market_data_stream.connection_status not in [
                                   StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR, StreamConnectionState.STOPPING
                               ]:
                                logger.debug(f"Bot: Updating token for DataStream (current state: {bot_instance.market_data_stream.connection_status.name})")
                                bot_instance.market_data_stream.update_token(latest_api_token)
                                
                    except AuthenticationError as e_auth_refresh:
                        logger.error("Bot: CRITICAL - Failed to get/refresh token from APIClient for stream updates: %s.", e_auth_refresh)
                        # Consider if bot should stop or try to recover after a delay
                    except Exception as e_token_mgmt: # pylint: disable=broad-except
                        logger.error("Bot: Error during periodic stream token management: %s",
                                     e_token_mgmt, exc_info=True)
                    last_stream_token_check_time = current_time_monotonic
            time.sleep(1) # Main loop sleep

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down bot...")
    except ConfigurationError as e_conf_main:
        logger.error("BOT CONFIGURATION ERROR: %s", e_conf_main)
    except AuthenticationError as e_auth_main:
        logger.error("BOT AUTHENTICATION FAILED: %s", e_auth_main)
    except APIResponseParsingError as e_parse_main:
        logger.error(f"BOT API RESPONSE PARSING ERROR (likely during auth or initial setup): {e_parse_main}")
        if e_parse_main.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse_main.raw_response_text[:500])
    except APIError as e_api_main:
        logger.error("BOT API ERROR: %s", e_api_main)
    except LibraryError as e_lib_main:
        logger.error("BOT LIBRARY ERROR: %s", e_lib_main)
    except ValueError as e_val_main: # e.g. invalid int conversion for account_id
        logger.error("BOT VALUE ERROR: %s", e_val_main)
    except Exception as e_crit_main: # pylint: disable=broad-except
        logger.critical("UNHANDLED CRITICAL ERROR in Trading Bot: %s",
                        e_crit_main, exc_info=True)
    finally:
        logger.info("Trading Bot: Attempting to stop streams...")
        if market_data_stream_instance: 
            status_name = market_data_stream_instance.connection_status.name if market_data_stream_instance.connection_status else "N/A"
            logger.info(f"Stopping Market Data Stream (current status: {status_name})...")
            market_data_stream_instance.stop(reason_for_stop="Bot shutdown")
        if user_hub_stream_instance: 
            status_name = user_hub_stream_instance.connection_status.name if user_hub_stream_instance.connection_status else "N/A"
            logger.info(f"Stopping User Hub Stream (current status: {status_name})...")
            user_hub_stream_instance.stop(reason_for_stop="Bot shutdown")
        logger.info("Trading Bot terminated.")

if __name__ == "__main__":
    main()