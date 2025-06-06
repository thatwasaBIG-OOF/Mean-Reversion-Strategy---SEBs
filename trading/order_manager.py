"""
Simplified order management with TSX API.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

from core.position import OrderInfo
from config.settings import OrderState

try:
    from tsxapipy import OrderPlacer
except ImportError:
    try:
        from tsxapipy.trading import OrderPlacer
    except ImportError:
        OrderPlacer = None

class OrderManager:
    """Simplified order management"""

    def __init__(self, api_client=None, account_id: Optional[str] = None, 
                 contract_id: Optional[str] = None, live_trading: bool = False):
        self.api_client = api_client
        self.account_id = int(account_id) if account_id else None
        self.contract_id = contract_id
        self.live_trading = live_trading
        self.order_placer = None
        self.logger = logging.getLogger(__name__)

        if self.live_trading:
            if not self.api_client:
                raise ValueError("API client required for live trading")
            self._initialize_order_placer()

    def _initialize_order_placer(self):
        """Initialize TSX API OrderPlacer"""
        if self.api_client and self.account_id and self.contract_id and OrderPlacer:
            try:
                self.order_placer = OrderPlacer(
                    api_client=self.api_client,
                    account_id=self.account_id,
                    default_contract_id=self.contract_id
                )
                self.logger.info("OrderPlacer initialized for live trading")
            except Exception as e:
                self.logger.error(f"Failed to initialize OrderPlacer: {e}")
                raise

    def place_limit_order(self, side: str, size: int, price: float) -> Optional[str]:
        """Place a limit order"""
        try:
            if self.live_trading and self.order_placer:
                order_id = self.order_placer.place_limit_order(
                    side=side.upper(),
                    size=size,
                    limit_price=price,
                    contract_id=self.contract_id
                )
                
                if order_id:
                    self.logger.info(f"Limit order placed: {side} {size} @ {price:.2f}, ID: {order_id}")
                    return str(order_id)
                else:
                    self.logger.error("Failed to place limit order")
                    return None
            else:
                # Simulation mode
                sim_id = f"SIM_{side}_{datetime.now().strftime('%H%M%S%f')}"
                self.logger.info(f"SIM Limit: {side} {size} @ {price:.2f}, ID: {sim_id}")
                return sim_id

        except Exception as e:
            self.logger.error(f"Error placing limit order: {e}")
            return None

    def place_stop_market_order(self, side: str, size: int, price: float) -> Optional[str]:
        """Place a stop market order"""
        try:
            if self.live_trading and self.order_placer:
                order_id = self.order_placer.place_stop_market_order(
                    side=side.upper(),
                    size=size,
                    stop_price=price,
                    contract_id=self.contract_id
                )
                
                if order_id:
                    self.logger.info(f"Stop order placed: {side} {size} @ {price:.2f}, ID: {order_id}")
                    return str(order_id)
                else:
                    self.logger.error("Failed to place stop order")
                    return None
            else:
                # Simulation mode
                sim_id = f"SIM_STOP_{side}_{datetime.now().strftime('%H%M%S%f')}"
                self.logger.info(f"SIM Stop: {side} {size} @ {price:.2f}, ID: {sim_id}")
                return sim_id

        except Exception as e:
            self.logger.error(f"Error placing stop order: {e}")
            return None

    def place_market_order(self, side: str, size: int) -> Optional[str]:
        """Place a market order"""
        try:
            if self.live_trading and self.order_placer:
                order_id = self.order_placer.place_market_order(
                    side=side.upper(),
                    size=size,
                    contract_id=self.contract_id
                )
                
                if order_id:
                    self.logger.info(f"Market order placed: {side} {size}, ID: {order_id}")
                    return str(order_id)
                else:
                    self.logger.error("Failed to place market order")
                    return None
            else:
                # Simulation mode
                sim_id = f"SIM_MKT_{side}_{datetime.now().strftime('%H%M%S%f')}"
                self.logger.info(f"SIM Market: {side} {size}, ID: {sim_id}")
                return sim_id

        except Exception as e:
            self.logger.error(f"Error placing market order: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order"""
        if not order_id:
            return False
            
        try:
            if self.live_trading and self.order_placer:
                if order_id.startswith('SIM_'):
                    self.logger.warning(f"Cannot cancel simulation order in live mode: {order_id}")
                    return False
                
                order_id_int = int(order_id)
                success = self.order_placer.cancel_order(order_id=order_id_int)
                
                if success:
                    self.logger.info(f"Order cancelled: {order_id}")
                else:
                    self.logger.error(f"Failed to cancel order: {order_id}")
                
                return success
            else:
                # Simulation mode
                self.logger.info(f"SIM Order cancelled: {order_id}")
                return True

        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id}: {e}")
            return False

    def modify_order_price(self, order_info: OrderInfo, new_price: float) -> bool:
        """Modify order price"""
        if not order_info.order_id:
            return False
            
        try:
            if self.live_trading and self.order_placer:
                if order_info.order_id.startswith('SIM_'):
                    return False
                
                order_id_int = int(order_info.order_id)
                
                # Use appropriate parameter based on order type
                if order_info.is_stop:
                    success = self.order_placer.modify_order(
                        order_id=order_id_int,
                        new_stop_price=new_price
                    )
                else:
                    success = self.order_placer.modify_order(
                        order_id=order_id_int,
                        new_limit_price=new_price
                    )
                
                if success:
                    order_info.price = new_price
                    self.logger.info(f"Order modified: {order_info.order_id} -> {new_price:.2f}")
                
                return success
            else:
                # Simulation mode
                order_info.price = new_price
                self.logger.info(f"SIM Order modified: {order_info.order_id} -> {new_price:.2f}")
                return True

        except Exception as e:
            self.logger.error(f"Error modifying order: {e}")
            return False

    def get_order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order status - simplified version"""
        if not order_id or not self.live_trading:
            return None
            
        try:
            # This would need to be implemented based on TSX API capabilities
            # For now, return None to indicate status checking should be done via UserHubStream
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting order status: {e}")
            return None

    def create_order_info(self, side: str, size: int, price: float, 
                         order_id: Optional[str] = None, is_stop: bool = False) -> OrderInfo:
        """Create an OrderInfo instance"""
        return OrderInfo(
            order_id=order_id,
            side=side,
            size=size,
            price=price,
            state=OrderState.PENDING if order_id else OrderState.NONE,
            is_stop=is_stop
        )