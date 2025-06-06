"""
Targeted logging setup to only capture detailed Pydantic errors
while keeping everything else quiet.
"""

import logging
import json
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


def setup_targeted_logging():
    """Setup logging to only show important errors, not debug spam"""
    
    # Silence the noisy TSX API loggers
    noisy_loggers = [
        'tsxapipy.real_time.data_stream',
        'tsxapipy.real_time.data_stream.DataStream',
        'tsxapipy',
        'websocket',
        'signalrcore',
        'urllib3',
        'requests'
    ]
    
    for logger_name in noisy_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.WARNING)  # Only warnings and errors
    
    # Keep our order manager at INFO level for important messages
    logging.getLogger('trading.order_manager').setLevel(logging.INFO)
    logging.getLogger(__name__).setLevel(logging.INFO)


class OrderManager:
    """Order management with targeted error logging - NO DEBUG SPAM"""

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

    def _log_critical_error_only(self, error: Exception, operation: str, **context):
        """Log ONLY critical errors with full details - no debug spam"""
        
        # Create an error file specifically for critical trading errors
        error_logger = logging.getLogger('CRITICAL_TRADING_ERRORS')
        if not error_logger.handlers:
            handler = logging.FileHandler('critical_trading_errors.log')
            formatter = logging.Formatter(
                '%(asctime)s - CRITICAL ERROR - %(message)s'
            )
            handler.setFormatter(formatter)
            error_logger.addHandler(handler)
            error_logger.setLevel(logging.ERROR)
        
        # Log to console with basic info
        self.logger.error(f"CRITICAL ERROR in {operation}: {type(error).__name__}: {str(error)}")
        
        # Log full details to file
        error_logger.error("=" * 80)
        error_logger.error(f"OPERATION: {operation}")
        error_logger.error(f"ERROR TYPE: {type(error).__name__}")
        error_logger.error(f"ERROR MESSAGE: {str(error)}")
        error_logger.error(f"TIMESTAMP: {datetime.now()}")
        
        # Log context
        if context:
            error_logger.error("CONTEXT:")
            try:
                context_json = json.dumps(context, indent=2, default=str)
                error_logger.error(context_json)
            except Exception:
                error_logger.error(f"Raw context: {context}")
        
        # Check for Pydantic ValidationError specifically
        try:
            from pydantic import ValidationError
            if isinstance(error, ValidationError):
                error_logger.error("PYDANTIC VALIDATION ERRORS:")
                for i, err in enumerate(error.errors(), 1):
                    error_logger.error(f"  {i}. Field: {err.get('loc', 'unknown')}")
                    error_logger.error(f"     Type: {err.get('type', 'unknown')}")
                    error_logger.error(f"     Message: {err.get('msg', 'unknown')}")
                    error_logger.error(f"     Input: {err.get('input', 'unknown')}")
        except ImportError:
            pass
        except Exception as detail_err:
            error_logger.error(f"Error parsing validation details: {detail_err}")
        
        error_logger.error("=" * 80)

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
            self._log_critical_error_only(
                e, "PLACE_LIMIT_ORDER",
                side=side, size=size, price=price, 
                contract_id=self.contract_id, account_id=self.account_id
            )
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
            self._log_critical_error_only(
                e, "PLACE_STOP_MARKET_ORDER",
                side=side, size=size, price=price,
                contract_id=self.contract_id, account_id=self.account_id
            )
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
            self._log_critical_error_only(
                e, "PLACE_MARKET_ORDER",
                side=side, size=size,
                contract_id=self.contract_id, account_id=self.account_id
            )
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
            self._log_critical_error_only(
                e, "CANCEL_ORDER",
                order_id=order_id, account_id=self.account_id
            )
            return False

    def modify_order_price(self, order_info: OrderInfo, new_price: float) -> bool:
        """Modify order price - ONLY logs critical errors, no debug spam"""
        if new_price is None or new_price <= 0:
            self.logger.error(f"Invalid new_price: {new_price}")
            return False
    
        if not order_info.order_id:
            return False
            
        try:
            if self.live_trading and self.order_placer:
                if order_info.order_id.startswith('SIM_'):
                    return False
                
                order_id_int = int(order_info.order_id)
                
                # Check if price actually changed
                if abs(new_price - order_info.price) < 0.01:
                    return True
                
                # Prepare parameters - only log if there's an error
                modify_params: Dict[str, Any] = {
                    "order_id": order_id_int,
                    "new_size": None,
                    "new_limit_price": None,
                    "new_stop_price": None,
                    "new_trail_price": None
                }
                
                if order_info.is_stop:
                    self.logger.info(f"Calling modify_order with order_id={order_id_int}, new_stop_price={new_price}")
                    success = self.order_placer.modify_order(
                        order_id=order_id_int,
                        new_stop_price=new_price,
                        new_limit_price=None,  # Explicitly set to None
                        new_size=None,
                        new_trail_price=None
        )
                else:
                    self.logger.info(f"Calling modify_order with order_id={order_id_int}, new_limit_price={new_price}")
                    success = self.order_placer.modify_order(
                        order_id=order_id_int,
                        new_limit_price=new_price,
                        new_stop_price=None,   # Explicitly set to None
                        new_size=None,
                        new_trail_price=None
                    )
                
                success = self.order_placer.modify_order(**modify_params)
                
                if success:
                    order_info.price = new_price
                    self.logger.info(f"Order modified: {order_info.order_id} -> {new_price:.2f}")
                else:
                    self.logger.warning(f"Failed to modify order {order_info.order_id}")
                
                return success
            else:
                # Simulation mode
                order_info.price = new_price
                self.logger.info(f"SIM Order modified: {order_info.order_id} -> {new_price:.2f}")
                return True

        except ValueError as e:
            self.logger.error(f"Order modification validation error: {e}")
            return False
        except Exception as e:
            # THIS is where the detailed Pydantic error logging happens
            self._log_critical_error_only(
                e, "MODIFY_ORDER",
                order_id=order_info.order_id,
                old_price=order_info.price,
                new_price=new_price,
                is_stop=order_info.is_stop,
                side=order_info.side,
                size=order_info.size,
                account_id=self.account_id,
                contract_id=self.contract_id,
                # Include the exact parameters that were sent
                modify_params={
                    "order_id": int(order_info.order_id) if not order_info.order_id.startswith('SIM_') else order_info.order_id,
                    "new_stop_price": new_price if order_info.is_stop else None,
                    "new_limit_price": new_price if not order_info.is_stop else None,
                    "new_size": None,
                    "new_trail_price": None
                }
            )
            return False

    def get_order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order status"""
        if not order_id or not self.live_trading:
            return None
            
        try:
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