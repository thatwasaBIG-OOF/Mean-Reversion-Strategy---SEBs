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
from tsxapipy.real_time import UserHubStream, DataStream
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
    ACCOUNT_ID_TO_WATCH as DEFAULT_CONFIG_ACCOUNT_ID,
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
)
from tsxapipy.common.time_utils import UTC_TZ
from tsxapipy.api.exceptions import ConfigurationError, LibraryError

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
            current_total_value = original_size * original_avg_price

            if (original_size > 0 and order_side_code == 0) or \
               (original_size < 0 and order_side_code == 1): # Adding to existing
                new_qty_for_avg_calc = fill_qty_this_event
                if original_size < 0 and order_side_code == 1: # Adding to short
                    new_total_value = current_total_value + \
                                      (-new_qty_for_avg_calc * float(fill_price_this_event))
                else: # Adding to long
                    new_total_value = current_total_value + \
                                      (new_qty_for_avg_calc * float(fill_price_this_event))
                self.size += fill_qty_this_event if order_side_code == 0 \
                                                 else -fill_qty_this_event
                if self.size != 0:
                    self.average_entry_price = new_total_value / self.size
                else:
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
                logger.info("Realized PnL from this fill: %.2f. Total PnL: %.2f",
                            pnl_this_fill, self.realized_pnl)
                if self.size == 0: # Position fully closed
                    self.average_entry_price = None
                    self.entry_order_ids = []
                elif (original_size > 0 and self.size < 0) or \
                     (original_size < 0 and self.size > 0): # Position flipped
                    self.average_entry_price = float(fill_price_this_event)
                    self.entry_order_ids = [order_id] if order_id else []

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
        self.active_orders: OrderedDict[int, Dict[str, Any]] = OrderedDict()
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
            logger.warning("Bot: _update_active_order with invalid order_id: %s",
                           order_data)
            return

        if order_id not in self.active_orders:
            if order_data.get("accountId") == self.account_id and \
               order_data.get("contractId") == self.contract_id:
                self.active_orders[order_id] = {}
                logger.info("Bot: Tracking new order %d (Type: %s, Side: %s, Size: %s).",
                            order_id, order_data.get('type'),
                            order_data.get('side'), order_data.get('size'))
            else:
                return

        updated_fields = {
            "id": order_id, "status": order_data.get("status"),
            "side": order_data.get("side"), "type": order_data.get("type"),
            "size": order_data.get("size"),
            "cumQuantity": order_data.get("cumQuantity", 0),
            "avgPx": order_data.get("avgPx"),
            "limitPrice": order_data.get("limitPrice"),
            "stopPrice": order_data.get("stopPrice"),
            "contractId": order_data.get("contractId"),
            "accountId": order_data.get("accountId"),
            "creationTimestamp": order_data.get("creationTimestamp"),
            "updateTimestamp": order_data.get("updateTimestamp"),
            "lastUpdateTimeBot": datetime.now(UTC_TZ)
        }
        if "leavesQuantity" in order_data:
            updated_fields["leavesQuantity"] = order_data.get("leavesQuantity")
        elif "size" in updated_fields and "cumQuantity" in updated_fields:
            updated_fields["leavesQuantity"] = \
                (updated_fields.get("size") or 0) - \
                (updated_fields.get("cumQuantity") or 0)

        self.active_orders[order_id].update(updated_fields)
        status_str = ORDER_STATUS_TO_STRING_MAP.get(updated_fields['status'], 'N/A')
        logger.debug("Bot: Updated active_orders for %d: Status=%s, CumQty=%s",
                     order_id, status_str, updated_fields['cumQuantity'])

    # pylint: disable=too-many-branches, too-many-statements
    def handle_realtime_order_update(self, order_data: Dict[str, Any]):
        """Handles order updates received from the UserHubStream."""
        order_id = order_data.get("id")
        if not order_id or order_data.get("accountId") != self.account_id or \
           order_data.get("contractId") != self.contract_id:
            return

        status_code = order_data.get("status")
        status_str = ORDER_STATUS_TO_STRING_MAP.get(
            status_code, f"UNKNOWN_STATUS({status_code})"
        )
        log_msg = ("[ORDER UPDATE EVENT] ID: %s, Status: %s, CumQty: %s, "
                   "LeavesQty: %s", order_id, status_str,
                   order_data.get('cumQuantity'), order_data.get('leavesQuantity'))
        logger.info(*log_msg)
        self._update_active_order(order_data)

        is_entry_order = (order_id == self.entry_order_id)
        # Check if it's an exit order currently tracked by the bot
        is_exit_order = (self.is_exiting_position and
                         self.active_orders.get(order_id) is not None)

        if status_code == ORDER_STATUS_FILLED:
            logger.info("Bot: Order %s FULLY FILLED! Position update via UserTrade.",
                        order_id)
            if is_entry_order:
                self.entry_order_id = None
            if is_exit_order and self.position.size == 0:
                self.is_exiting_position = False
            if order_id in self.active_orders:
                del self.active_orders[order_id]
        elif status_code == ORDER_STATUS_PARTIALLY_FILLED:
            logger.info("Bot: Order %s PARTIALLY FILLED. Awaiting UserTrade.",
                        order_id)
        elif status_code in [ORDER_STATUS_CANCELLED, ORDER_STATUS_REJECTED,
                             ORDER_STATUS_EXPIRED]:
            logger.info("Bot: Order %s (%s) is terminal without/before full fill.",
                        order_id, status_str)
            if is_entry_order:
                self.entry_order_id = None
                self.last_signal_decision = None
            if is_exit_order:
                self.is_exiting_position = False
                logger.warning("Bot: Exit order %s %s! Position: %d. May need action.",
                               order_id, status_str, self.position.size)
            if order_id in self.active_orders:
                del self.active_orders[order_id]
        elif status_code in [ORDER_STATUS_WORKING, ORDER_STATUS_PENDING_NEW]:
            logger.debug("Bot: Order %s is active: %s.", order_id, status_str)
        else:
            logger.warning("Bot: Order %s has unhandled status by bot: %s (%s).",
                           order_id, status_str, status_code)

        log_state = ("Bot State After Order Update: PosSize=%d, EntryOrderID=%s, "
                     "IsExiting=%s, ActiveOrdersCount=%d", self.position.size,
                     self.entry_order_id, self.is_exiting_position,
                     len(self.active_orders))
        logger.debug(*log_state)

    def handle_account_update(self, acc_data: Dict[str, Any]):
        """Handles account updates from UserHubStream."""
        if acc_data.get("id") == self.account_id:
            logger.info("Bot: Received AccountUpdate for watched account %s: Balance=%s",
                        self.account_id, acc_data.get('balance'))
        else:
            logger.debug("Bot: Received AccountUpdate for other account: %s",
                         acc_data.get('id'))

    def handle_position_update(self, pos_data: Dict[str, Any]):
        """Handles position updates from UserHubStream (less authoritative than trades)."""
        if pos_data.get("accountId") == self.account_id and \
           pos_data.get("contractId") == self.contract_id:
            log_msg = ("Bot: Received PositionUpdate from stream for %s: Size=%s, "
                       "AvgPx=%s", self.contract_id, pos_data.get('size'),
                       pos_data.get('averagePrice'))
            logger.info(*log_msg)

    # pylint: disable=too-many-branches
    def handle_user_trade_update(self, trade_data: Dict[str, Any]):
        """Handles user trade execution updates from UserHubStream."""
        if trade_data.get("accountId") == self.account_id and \
           trade_data.get("contractId") == self.contract_id:
            order_id_from_trade = trade_data.get("orderId")
            trade_side_code = trade_data.get("side") # 0=Buy, 1=Sell
            trade_qty = trade_data.get("size")
            trade_price = trade_data.get("price")

            log_msg = ("[USER TRADE EXECUTION] OrderID: %s, SideCode: %s, Qty: %s, "
                       "Price: %s, PnL (API): %s", order_id_from_trade,
                       trade_side_code, trade_qty, trade_price,
                       trade_data.get('profitAndLoss'))
            logger.info(*log_msg)

            if trade_qty is not None and trade_price is not None and \
               trade_side_code is not None:
                self.position.update_on_fill(
                    fill_qty_this_event=int(trade_qty),
                    fill_price_this_event=float(trade_price),
                    order_side_code=int(trade_side_code),
                    order_id=order_id_from_trade
                )
                if self.is_exiting_position and self.position.size == 0:
                    logger.info("Bot: Position flat after UserTrade. "
                                "Clearing is_exiting_position flag.")
                    self.is_exiting_position = False
            else:
                logger.warning("Bot: Incomplete data in UserTrade for OrderID %s. "
                               "Cannot update position accurately from this event.",
                               order_id_from_trade)
        else:
            logger.debug("Bot: Received UserTradeUpdate for other account/contract: %s",
                         trade_data.get('orderId'))

    def handle_market_quote_payload(self, quote_data: Dict[str, Any]):
        """Handles market quote payloads from DataStream."""
        logger.debug("Bot: Received MarketQuote (not used by SMA): %s/%s",
                     quote_data.get('bp'), quote_data.get('ap'))

    def handle_market_depth_payload(self, depth_data: Any): # pylint: disable=unused-argument
        """Handles market depth payloads from DataStream."""
        logger.debug("Bot: Received MarketDepth (not used by bot)")

    def _can_place_new_entry_order(self) -> bool:
        """Checks if conditions allow placing a new entry order."""
        if self.entry_order_id and self.entry_order_id in self.active_orders:
            logger.debug("Bot: Cannot place new entry: existing entry order active.")
            return False
        if self.is_exiting_position:
            logger.debug("Bot: Cannot place new entry: currently exiting position.")
            return False
        return True

    def _attempt_entry(self, decision: str):
        """Attempts to enter a position based on the trading decision."""
        if not self._can_place_new_entry_order():
            return

        if self.position.size != 0:
            logger.info("Bot: Signal to %s, but position is %d. Attempting exit.",
                        decision, self.position.size)
            self._attempt_exit_position()
            return

        logger.info("Bot: Attempting to ENTER %s position for %s, Size: 1.",
                    decision, self.contract_id)
        order_side_mapped = ORDER_SIDES_MAP.get(decision.upper())
        if order_side_mapped is None:
            logger.error("Bot: Invalid decision '%s' for entry attempt.", decision)
            return

        order_id = self.order_placer.place_market_order(side=decision.upper(), size=1)

        if order_id:
            logger.info("Bot: Market %s order to ENTER placed. Order ID: %s. "
                        "Awaiting stream confirmation.", decision, order_id)
            self.entry_order_id = order_id
            self._update_active_order({
                "id": order_id, "status": ORDER_STATUS_PENDING_NEW,
                "side": order_side_mapped, "type": ORDER_TYPES_MAP["MARKET"],
                "size": 1, "contractId": self.contract_id,
                "accountId": self.account_id,
                "creationTimestamp": datetime.now(UTC_TZ).isoformat()
            })
            self.last_signal_decision = decision
        else:
            logger.error("Bot: Failed to place market %s order to enter (Op returned None).",
                         decision)
            self.last_signal_decision = None

    def _attempt_exit_position(self):
        """Attempts to exit the current position by placing a market order."""
        if self.position.size == 0:
            logger.debug("Bot: No position to exit.")
            return
        if self.is_exiting_position and \
           any(o.get("status") in [ORDER_STATUS_PENDING_NEW, ORDER_STATUS_WORKING]
               for o in self.active_orders.values()):
            logger.info("Bot: Already attempting to exit with an active order.")
            return

        exit_side_str = "SELL" if self.position.size > 0 else "BUY"
        exit_size = abs(self.position.size)
        logger.info("Bot: Attempting to EXIT position of %d for %s with %s size %d.",
                    self.position.size, self.contract_id, exit_side_str, exit_size)

        order_id = self.order_placer.place_market_order(side=exit_side_str.upper(),
                                                        size=exit_size)
        if order_id:
            self.is_exiting_position = True
            logger.info("Bot: Market %s order to EXIT placed. Order ID: %s.",
                        exit_side_str, order_id)
            exit_side_mapped = ORDER_SIDES_MAP.get(exit_side_str.upper())
            self._update_active_order({
                "id": order_id, "status": ORDER_STATUS_PENDING_NEW,
                "side": exit_side_mapped, "type": ORDER_TYPES_MAP["MARKET"],
                "size": exit_size, "contractId": self.contract_id,
                "accountId": self.account_id,
                "creationTimestamp": datetime.now(UTC_TZ).isoformat()
            })
        else:
            logger.error("Bot: Failed to place market %s order to exit position.",
                         exit_side_str)
            self.is_exiting_position = False

    # pylint: disable=too-many-branches
    def process_market_data_tick(self, trade_data_item: Any):
        """Processes a single market data tick (trade) to make trading decisions."""
        if not isinstance(trade_data_item, dict):
            return
        price_value = trade_data_item.get('price') or trade_data_item.get('p')
        if price_value is None:
            return
        try:
            price = float(price_value)
        except (ValueError, TypeError):
            logger.warning("Bot: Invalid price data in tick: %s", price_value)
            return

        self.price_history.append(price)
        if len(self.price_history) < self.sma_period:
            logger.debug("Bot: Not enough history (%d/%d) for MA.",
                         len(self.price_history), self.sma_period)
            return

        ma_val = simple_moving_average(list(self.price_history), self.sma_period)
        if ma_val is None:
            logger.warning("Bot: Could not calculate SMA.")
            return

        current_signal = decide_trade(price, ma_val)
        log_tick = ("Bot Tick: Px=%.2f, MA(%d)=%.2f, Signal=%s, PosSize=%d, "
                    "EntryID=%s, ExitFlag=%s", price, self.sma_period, ma_val,
                    current_signal, self.position.size, self.entry_order_id,
                    self.is_exiting_position)
        logger.info(*log_tick)

        if self.is_exiting_position and \
           any(o.get("status") in [ORDER_STATUS_PENDING_NEW, ORDER_STATUS_WORKING]
               for o in self.active_orders.values()):
            logger.debug("Bot: Currently exiting, no new trade decisions on tick.")
            return

        if self.position.size == 0:
            if current_signal == "BUY" and self.last_signal_decision != "BUY":
                self._attempt_entry("BUY")
            elif current_signal == "SELL" and self.last_signal_decision != "SELL":
                self._attempt_entry("SELL")
        elif self.position.size > 0 and current_signal == "SELL":
            logger.info("Bot: SELL signal while LONG. Attempting to exit.")
            self._attempt_exit_position()
        elif self.position.size < 0 and current_signal == "BUY":
            logger.info("Bot: BUY signal while SHORT. Attempting to exit.")
            self._attempt_exit_position()

    def handle_market_data_payload(self, market_payload: Any):
        """Handles the overall market data payload from DataStream."""
        if isinstance(market_payload, list):
            for item in market_payload:
                self.process_market_data_tick(item)
        elif isinstance(market_payload, dict):
            self.process_market_data_tick(market_payload)

    # pylint: disable=too-many-branches
    def poll_active_orders_status(self, search_window_minutes: int = 30):
        """Periodically polls the status of tracked active orders."""
        if not self.active_orders:
            logger.debug("Bot: No active orders to poll.")
            return

        logger.info("Bot: Polling status for %d active order(s)...",
                    len(self.active_orders))
        for order_id in list(self.active_orders.keys()): # Iterate over copy of keys
            if order_id not in self.active_orders: # Check if removed during iteration
                continue

            order_snapshot = self.active_orders[order_id]
            last_bot_update_time = order_snapshot.get(
                "lastUpdateTimeBot", datetime.now(UTC_TZ) - timedelta(days=1)
            )
            force_poll = False
            pending_new_timeout = timedelta(seconds=30)
            working_stale_timeout = timedelta(seconds=STALE_ORDER_THRESHOLD_SECONDS)

            if order_snapshot.get("status") == ORDER_STATUS_PENDING_NEW and \
               (datetime.now(UTC_TZ) - order_snapshot.get("creationTimestamp",
                                                           last_bot_update_time)
                ) > pending_new_timeout:
                logger.warning("Bot: Order %s PENDING_NEW for >30s. Forcing poll.",
                               order_id)
                force_poll = True
            elif order_snapshot.get("status") == ORDER_STATUS_WORKING and \
                 (datetime.now(UTC_TZ) - last_bot_update_time) > working_stale_timeout:
                log_stale = ("Bot: Order %s status %s seems stale. Forcing poll.",
                             order_id, ORDER_STATUS_TO_STRING_MAP.get(
                                 order_snapshot.get('status')))
                logger.warning(*log_stale)
                force_poll = True

            if not force_poll and \
               (datetime.now(UTC_TZ) - last_bot_update_time) < \
               timedelta(seconds=ORDER_POLL_INTERVAL_SECONDS / 2):
                continue

            try:
                logger.debug("Bot: Polling details for order %s...", order_id)
                live_details = self.order_placer.get_order_details(
                    order_id, search_window_minutes=search_window_minutes
                )
                if live_details:
                    changed_status = order_snapshot.get("status") != \
                                     live_details.get("status")
                    changed_cum_qty = order_snapshot.get("cumQuantity") != \
                                      live_details.get("cumQuantity")
                    if changed_status or changed_cum_qty:
                        log_poll_upd = ("[POLL UPDATE] Order ID %s changed. "
                                        "Status: %s->%s. CumQty: %s->%s",
                                        order_id, order_snapshot.get('status'),
                                        live_details.get('status'),
                                        order_snapshot.get('cumQuantity'),
                                        live_details.get('cumQuantity'))
                        logger.info(*log_poll_upd)
                        self.handle_realtime_order_update(live_details)
                    elif order_id in self.active_orders: # If no change, update bot time
                        self.active_orders[order_id]["lastUpdateTimeBot"] = \
                            datetime.now(UTC_TZ)
                elif order_snapshot.get("status") in \
                     [ORDER_STATUS_PENDING_NEW, ORDER_STATUS_WORKING]:
                    log_poll_notfound = ("[POLL NOT FOUND] Active order %s (status: %s) "
                                         "not found. Removing from active list.",
                                         order_id, ORDER_STATUS_TO_STRING_MAP.get(
                                             order_snapshot.get('status')))
                    logger.warning(*log_poll_notfound)
                    self.handle_realtime_order_update({
                        "id": order_id, "status": ORDER_STATUS_UNKNOWN,
                        "accountId": self.account_id,
                        "contractId": self.contract_id
                    })
            except Exception as e: # pylint: disable=broad-exception-caught
                logger.error("Bot: Error polling status for order %s: %s",
                             order_id, e, exc_info=True)

    # pylint: disable=too-many-branches
    def reconcile_position(self):
        """Reconciles the bot's tracked position with the API's open positions."""
        log_msg = ("Bot: Attempting to reconcile position for Account %s, Contract %s...",
                   self.account_id, self.contract_id)
        logger.info(*log_msg)
        try:
            open_positions_api = self.api_client.search_open_positions(
                account_id=self.account_id
            )
            api_pos_for_contract: Optional[Dict[str, Any]] = None
            for pos in open_positions_api:
                if pos.get("contractId") == self.contract_id:
                    api_pos_for_contract = pos
                    break

            if api_pos_for_contract:
                api_size = api_pos_for_contract.get("size", 0)
                api_avg_px_str = api_pos_for_contract.get("averagePrice")
                api_avg_px = float(api_avg_px_str) if api_avg_px_str else None
                logger.info("Bot: API Position for %s: Size=%s, AvgPrice=%s",
                            self.contract_id, api_size, api_avg_px)

                bot_avg_px = self.position.average_entry_price
                mismatch = (api_size != self.position.size or
                            (api_avg_px is not None and bot_avg_px is not None and
                             abs(api_avg_px - bot_avg_px) > 0.0001) or
                            (api_avg_px is None and bot_avg_px is not None) or
                            (api_avg_px is not None and bot_avg_px is None and api_size !=0))

                if mismatch:
                    log_mismatch = ("Bot: POSITION RECONCILIATION MISMATCH for %s: "
                                    "API Size=%s (AvgPx=%s) vs Bot Size=%s (AvgPx=%s). "
                                    "UPDATING BOT STATE.", self.contract_id, api_size,
                                    api_avg_px, self.position.size, bot_avg_px)
                    logger.warning(*log_mismatch)
                    self.position.size = api_size
                    self.position.average_entry_price = api_avg_px
                    if self.position.size == 0:
                        self.position.entry_order_ids = []
                        self.entry_order_id = None
                        self.is_exiting_position = False
                    elif not self.position.entry_order_ids:
                        logger.warning("Bot: Position exists on API (Size: %d), "
                                       "but bot has no entry order IDs tracked.",
                                       self.position.size)
                else:
                    logger.info("Bot: Position for %s matches API (Size: %d, AvgPx: %s).",
                                self.contract_id, self.position.size,
                                self.position.average_entry_price)
            elif self.position.size != 0: # API shows no position, bot does
                log_no_api_pos = ("Bot: POSITION RECONCILIATION: API shows NO open "
                                  "position for %s, but bot state is Size=%d. "
                                  "RESETTING BOT POSITION TO FLAT.", self.contract_id,
                                  self.position.size)
                logger.warning(*log_no_api_pos)
                self.position.size = 0
                self.position.average_entry_price = None
                self.position.entry_order_ids = []
                self.is_exiting_position = False
                self.entry_order_id = None
            else: # Both API and bot show no position
                logger.info("Bot: POSITION RECONCILIATION: Bot and API both show no "
                            "open position for %s.", self.contract_id)
        except APIError as e:
            logger.error("Bot: API Error during position reconciliation: %s", e)
        except Exception as e_rec: # pylint: disable=broad-exception-caught
            logger.error("Bot: Unexpected error during position reconciliation: %s",
                         e_rec, exc_info=True)

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
                        default=DEFAULT_CONFIG_ACCOUNT_ID,
                        help="Account ID for trading.")
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
                        dest="stream_account", default=True)
    parser.add_argument("--no_position_stream", action="store_false",
                        dest="stream_positions", default=True)
    parser.add_argument("--no_user_trade_stream", action="store_false",
                        dest="stream_user_trades", default=True)
    parser.add_argument("--no_quote_stream", action="store_false",
                        dest="stream_quotes", default=True)
    parser.add_argument("--no_market_trade_stream", action="store_false",
                        dest="stream_market_trades", default=True)
    parser.add_argument("--no_depth_stream", action="store_false",
                        dest="stream_depth", default=True)
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG level logging.")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True
    )
    if args.debug:
        logger.info("DEBUG logging enabled for all modules.")
        # logging.getLogger("SignalRCoreClient").setLevel(logging.DEBUG) # If needed

    if not args.contract_id:
        logger.error("Contract ID (--contract_id) is required. Exiting.")
        sys.exit(1)
    if not args.account_id or args.account_id <= 0:
        logger.error("A valid positive Account ID (--account_id) is required. Exiting.")
        sys.exit(1)

    logger.info("Starting Trading Bot: Contract=%s, Account=%s, SMA Period=%d",
                args.contract_id, args.account_id, args.sma_period)

    api_client_instance: Optional[APIClient] = None
    bot_instance: Optional[TradingBot] = None
    user_hub_stream_instance: Optional[UserHubStream] = None # For finally block
    market_data_stream_instance: Optional[DataStream] = None # For finally block

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
            on_account_update=(bot_instance.handle_account_update
                               if args.stream_account else None),
            on_position_update=(bot_instance.handle_position_update
                                if args.stream_positions else None),
            on_user_trade_update=(bot_instance.handle_user_trade_update
                                  if args.stream_user_trades else None),
            subscribe_to_accounts_globally=args.stream_account,
            on_state_change_callback=lambda state: logger.info(
                "Bot's UserHubStream state: %s", state),
            on_error_callback=lambda err: logger.error(
                "Bot's UserHubStream error: %s", err)
        )
        bot_instance.user_hub_stream = user_hub_stream_instance
        if not user_hub_stream_instance.start():
            logger.error("Failed to start UserHubStream for bot! Exiting.")
            sys.exit(1)

        logger.info("Initializing DataStream for bot...")
        market_data_stream_instance = DataStream(
            api_client=api_client_instance,
            contract_id_to_subscribe=args.contract_id,
            on_trade_callback=(bot_instance.handle_market_data_payload
                               if args.stream_market_trades else None),
            on_quote_callback=(bot_instance.handle_market_quote_payload
                               if args.stream_quotes else None),
            on_depth_callback=(bot_instance.handle_market_depth_payload
                               if args.stream_depth else None),
            on_state_change_callback=lambda state: logger.info(
                "Bot's DataStream state: %s", state),
            on_error_callback=lambda err: logger.error(
                "Bot's DataStream error: %s", err)
        )
        bot_instance.market_data_stream = market_data_stream_instance
        if not market_data_stream_instance.start():
            logger.error("Failed to start DataStream for bot! Exiting.")
            sys.exit(1)

        logger.info("Trading bot running... Press Ctrl+C to stop.")
        last_order_poll_time = time.monotonic()
        last_position_reconcile_time = time.monotonic()
        stream_token_refresh_interval_seconds = \
            (TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES * 60) / 3
        last_stream_token_check_time = time.monotonic()

        bot_instance.reconcile_position()

        while True:
            current_time_monotonic = time.monotonic()
            if bot_instance: # Check bot_instance exists
                if current_time_monotonic - last_order_poll_time >= \
                   args.poll_interval_seconds:
                    bot_instance.poll_active_orders_status()
                    last_order_poll_time = current_time_monotonic
                if current_time_monotonic - last_position_reconcile_time >= \
                   args.reconcile_interval_seconds:
                    bot_instance.reconcile_position()
                    last_position_reconcile_time = current_time_monotonic
                if current_time_monotonic - last_stream_token_check_time >= \
                   stream_token_refresh_interval_seconds:
                    logger.debug("Bot: Periodic check to update stream tokens if needed.")
                    try:
                        if api_client_instance:
                            latest_api_token = api_client_instance.current_token
                            if bot_instance.user_hub_stream:
                                bot_instance.user_hub_stream.update_token(
                                    latest_api_token)
                            if bot_instance.market_data_stream:
                                bot_instance.market_data_stream.update_token(
                                    latest_api_token)
                    except AuthenticationError as e_auth:
                        logger.error("Bot: CRITICAL - Failed to refresh token "
                                     "for streams: %s.", e_auth)
                    except Exception as e_token: # pylint: disable=broad-exception-caught
                        logger.error("Bot: Error during stream token management: %s",
                                     e_token, exc_info=True)
                    last_stream_token_check_time = current_time_monotonic
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down bot...")
    except ConfigurationError as e:
        logger.error("BOT CONFIGURATION ERROR: %s", e)
    except AuthenticationError as e:
        logger.error("BOT AUTHENTICATION FAILED: %s", e)
    except APIError as e:
        logger.error("BOT API ERROR: %s", e)
    except LibraryError as e:
        logger.error("BOT LIBRARY ERROR: %s", e)
    except ValueError as e:
        logger.error("BOT VALUE ERROR: %s", e)
    except Exception as e_crit: # pylint: disable=broad-exception-caught
        logger.critical("UNHANDLED CRITICAL ERROR in Trading Bot: %s",
                        e_crit, exc_info=True)
    finally:
        logger.info("Trading Bot: Attempting to stop streams...")
        if market_data_stream_instance: # Use correct variable name
            logger.info("Stopping Market Data Stream...")
            market_data_stream_instance.stop()
        if user_hub_stream_instance: # Use correct variable name
            logger.info("Stopping User Hub Stream...")
            user_hub_stream_instance.stop()
        logger.info("Trading Bot terminated.")

if __name__ == "__main__":
    main()