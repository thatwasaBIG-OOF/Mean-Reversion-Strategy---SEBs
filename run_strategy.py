"""
Main entry point for the mean reversion trading strategy.
Simplified and streamlined version.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import logging
import time
import signal
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

from tsxapipy.auth import authenticate
from tsxapipy.api.client import APIClient
from tsxapipy.real_time.data_stream import DataStream
from tsxapipy.real_time.stream_state import StreamConnectionState
from tsxapipy.real_time.user_hub_stream import UserHubStream

from config.settings import StrategyConfig, TradingConfig
from core.strategy import MeanReversionStrategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Reduce noise from external libraries
for lib in ['tsxapipy', 'websocket', 'signalrcore', 'urllib3', 'requests']:
    logging.getLogger(lib).setLevel(logging.WARNING)

# Global variables
strategy: Optional[MeanReversionStrategy] = None
data_stream: Optional[DataStream] = None
user_stream: Optional[UserHubStream] = None
running = True


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logger.info("Shutdown signal received...")
    running = False


def handle_market_data(market_data: Dict[str, Any]):
    """Process incoming market data"""
    if strategy and running:
        strategy.process_tick({
            'price': market_data.get('price', 0),
            'timestamp': market_data.get('timestamp'),
            'volume': market_data.get('volume', 0)
        })


def handle_order_update(order_update):
    """Handle order updates from UserHubStream"""
    if strategy:
        strategy.handle_order_update(order_update)


def handle_position_update(position_update):
    """Handle position updates from UserHubStream"""
    if strategy:
        strategy.handle_position_update(position_update)


def handle_stream_state_change(state_str: str):
    """Handle stream state changes"""
    if isinstance(state_str, str):
        logger.info(f"Stream state: {state_str}")
    else:
        logger.warning(f"Unexpected state type: {type(state_str)} - {state_str}")


def handle_stream_error(error: Any):
    """Handle stream errors"""
    if error:
        logger.error(f"Stream error: {type(error).__name__} - {str(error)[:200]}")
    else:
        logger.error("Stream error with no details")


def print_commands():
    """Print available commands"""
    print("\nCommands: [Q]uit | [F]latten | [C]ancel | [S]tatus | [H]elp")


def handle_user_input():
    """Process user commands"""
    global running
    
    try:
        import select
        if select.select([sys.stdin], [], [], 0)[0]:
            cmd = input().strip().lower()
            
            if cmd in ['q', 'quit']:
                running = False
                print("Shutting down...")
            elif cmd in ['f', 'flatten'] and strategy:
                print("Flattening all positions...")
                if strategy.flatten_all_positions():
                    print("✅ Positions flattened")
            elif cmd in ['c', 'cancel'] and strategy:
                print("Cancelling orders...")
                if strategy.cancel_all_pending_orders():
                    print("✅ Orders cancelled")
            elif cmd in ['s', 'status'] and strategy:
                print_status()
            elif cmd in ['h', 'help']:
                print_commands()
    except:
        pass


def print_status():
    """Print current strategy status"""
    if not strategy:
        return
        
    status = strategy.get_status()
    
    print("\n" + "=" * 50)
    print("STRATEGY STATUS")
    print("=" * 50)
    
    # Position info
    if status['position_side'] != 'FLAT':
        print(f"Position: {status['position_side']} {status['position_size']} @ {status['entry_price']:.2f}")
        print(f"P&L: {status['live_pnl_points']:+.2f} pts (${status['live_pnl_dollars']:+.0f})")
        if status.get('stop_loss'):
            print(f"Stop Loss: {status['stop_loss']:.2f}")
    else:
        print("Position: FLAT")
    
    # Performance
    print(f"\nTrades: {status['total_trades']} | Win Rate: {status['win_rate']:.1f}%")
    print(f"Daily P&L: {status['daily_pnl']:+.2f} pts")
    
    # SEB info
    if status.get('seb_bands'):
        bands = status['seb_bands']
        print(f"\nSEB: L={bands.get('lower_band', 0):.2f} | "
              f"M={bands.get('mean', 0):.2f} | "
              f"U={bands.get('upper_band', 0):.2f}")
    
    print("=" * 50)


def select_account(api_client: APIClient) -> Optional[int]:
    """Select trading account"""
    try:
        accounts = api_client.get_accounts(only_active=True)
        if not accounts:
            logger.error("No active accounts found")
            return None
        
        print("\nAvailable Accounts:")
        for i, account in enumerate(accounts, 1):
            print(f"{i}. {account.name} (ID: {account.id})")
        
        while True:
            try:
                choice = int(input(f"Select account (1-{len(accounts)}): "))
                if 1 <= choice <= len(accounts):
                    return accounts[choice - 1].id
                print("Invalid selection")
            except ValueError:
                print("Please enter a number")
            except KeyboardInterrupt:
                return None
                
    except Exception as e:
        logger.error(f"Error selecting account: {e}")
        return None


def select_contract(api_client: APIClient) -> Optional[str]:
    """Select trading contract"""
    # Common futures contracts
    common_contracts = {
        "1": ("ES", "E-mini S&P 500"),
        "2": ("MES", "Micro E-mini S&P 500"),
        "3": ("NQ", "E-mini NASDAQ-100"),
        "4": ("MNQ", "Micro E-mini NASDAQ-100"),
        "5": ("YM", "E-mini Dow"),
        "6": ("RTY", "E-mini Russell 2000"),
        "7": ("CL", "Crude Oil"),
        "8": ("GC", "Gold"),
    }
    
    print("\nSelect Contract:")
    for key, (symbol, desc) in common_contracts.items():
        print(f"{key}. {symbol} - {desc}")
    print("9. Custom symbol")
    
    choice = input("Selection: ").strip()
    
    if choice in common_contracts:
        symbol = common_contracts[choice][0]
    elif choice == "9":
        symbol = input("Enter symbol: ").strip().upper()
    else:
        print("Invalid selection")
        return None
    
    # Search for contract
    print(f"Searching for {symbol} contracts...")
    contracts = api_client.search_contracts(search_text=symbol, live=False)
    
    if not contracts:
        print(f"No contracts found for {symbol}")
        return None
    
    # Filter and display relevant contracts
    relevant_contracts = []
    for contract in contracts:
        contract_id = str(contract.id)
        if symbol in contract_id or symbol in getattr(contract, 'name', ''):
            relevant_contracts.append(contract)
    
    if not relevant_contracts:
        print(f"No relevant {symbol} contracts found")
        return None
    
    # Display contracts
    print(f"\nAvailable {symbol} contracts:")
    for i, contract in enumerate(relevant_contracts[:10], 1):
        print(f"{i}. {contract.id} - {getattr(contract, 'name', 'N/A')}")
    
    while True:
        try:
            choice = int(input(f"Select contract (1-{min(10, len(relevant_contracts))}): "))
            if 1 <= choice <= min(10, len(relevant_contracts)):
                return relevant_contracts[choice - 1].id
            print("Invalid selection")
        except ValueError:
            print("Please enter a number")
        except KeyboardInterrupt:
            return None


def select_mode() -> bool:
    """Select trading mode"""
    print("\nTrading Mode:")
    print("1. Simulation (Safe)")
    print("2. Live Trading (Real Money)")
    
    choice = input("Select mode (1-2): ").strip()
    
    if choice == "2":
        confirm = input("Type 'CONFIRM' for live trading: ").strip()
        if confirm == "CONFIRM":
            print("⚠️  LIVE TRADING ENABLED")
            return True
    
    print("✅ Simulation mode")
    return False


def main():
    """Main entry point"""
    global strategy, data_stream, user_stream, running
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 50)
    print("MEAN REVERSION TRADING STRATEGY")
    print("=" * 50)
    
    try:
        # Authenticate
        print("\n🔐 Authenticating...")
        token, token_time = authenticate()
        if not token:
            raise ValueError("Authentication failed")
            
        api_client = APIClient(token, token_time)
        print("✅ Authenticated")
        
        # Select account
        account_id = select_account(api_client)
        if not account_id:
            return
            
        # Select contract
        contract_id = select_contract(api_client)
        if not contract_id:
            return
            
        # Select mode
        live_trading = select_mode()
        
        # Select strategy config
        print("\nStrategy Configuration:")
        print("1. ES Default")
        print("2. NQ Default")
        print("3. Custom")
        
        config_choice = input("Select config (1-3): ").strip()
        
        if config_choice == "1":
            strategy_config = StrategyConfig.create_futures_config("ES")
        elif config_choice == "2":
            strategy_config = StrategyConfig.create_futures_config("NQ")
        else:
            strategy_config = StrategyConfig.create_futures_config("ES")
            # Allow customization here if needed
        
        # Initialize strategy
        print("\n🔧 Initializing strategy...")
        trading_config = TradingConfig(
            api_client=api_client,
            account_id=str(account_id),
            contract_id=contract_id,
            live_trading=live_trading
        )
        
        strategy = MeanReversionStrategy(strategy_config, trading_config)
        print("✅ Strategy initialized")
        
        # Start UserHubStream for live trading
        if live_trading:
            print("\n📡 Starting order stream...")
            user_stream = UserHubStream(
                api_client=api_client,
                account_id_to_watch=account_id,
                on_order_update=handle_order_update,
                on_position_update=handle_position_update,
                subscribe_to_accounts_globally=False,
                on_state_change_callback=handle_stream_state_change,
                on_error_callback=handle_stream_error
            )
            
            if user_stream.start():
                print("✅ Order stream started")
            else:
                print("⚠️  Order stream failed - continuing without real-time updates")
                user_stream = None
        
        # Start market data stream
        print("\n📊 Starting market data...")
        try:
            data_stream = DataStream(
                api_client=api_client,
                contract_id_to_subscribe=contract_id,
                on_trade_callback=handle_market_data,
                on_quote_callback=None,  # Explicitly set to None
                on_depth_callback=None,   # Explicitly set to None
                on_state_change_callback=handle_stream_state_change,
                on_error_callback=handle_stream_error,
                auto_subscribe_trades=True,
                auto_subscribe_quotes=False,
                auto_subscribe_depth=False
            )
            
            if not data_stream.start():
                raise ValueError("Failed to start market data stream")
                
            print("✅ Market data started")
        except Exception as e:
            logger.error(f"Failed to initialize data stream: {e}")
            raise
        print("\n🚀 Strategy is running!")
        print_commands()
        
        # Main loop
        last_status_time = time.time()
        status_interval = 30  # Status every 30 seconds
        
        while running:
            try:
                # Handle user input
                handle_user_input()
                
                # Periodic status update
                if time.time() - last_status_time > status_interval:
                    if strategy:
                        status = strategy.get_status()
                        if status['position_side'] != 'FLAT':
                            print(f"\n📊 {status['position_side']} {status['position_size']} | "
                                  f"P&L: {status['live_pnl_points']:+.2f} (${status['live_pnl_dollars']:+.0f})")
                    last_status_time = time.time()
                
                # Check stream health
                if data_stream and data_stream.connection_status == StreamConnectionState.ERROR:
                    logger.warning("Reconnecting data stream...")
                    data_stream.start()
                
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)
                
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        # Cleanup
        logger.info("Shutting down...")
        
        # Stop streams gracefully
        if data_stream:
            try:
                logger.info("Stopping data stream...")
                data_stream.stop("Strategy shutdown")
                # Give it time to close properly
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error stopping data stream: {e}")
            
        if user_stream:
            try:
                logger.info("Stopping user stream...")
                user_stream.stop("Strategy shutdown")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error stopping user stream: {e}")
            
        if strategy:
            try:
                # Ensure all positions are flat
                if strategy.get_status()['position_side'] != 'FLAT':
                    logger.info("Flattening positions before shutdown...")
                    strategy.flatten_all_positions()
                    time.sleep(2)  # Give orders time to execute
                
                # Final summary
                summary = strategy.get_performance_summary()
                print("\n" + "=" * 50)
                print("FINAL SUMMARY")
                print("=" * 50)
                print(f"Total Trades: {summary['total_trades']}")
                print(f"Win Rate: {summary['win_rate']:.1f}%")
                print(f"Total P&L: {summary['total_pnl_points']:+.2f} pts (${summary['total_pnl_dollars']:+.0f})")
                print("=" * 50)
            except Exception as e:
                logger.error(f"Error in final summary: {e}")
        
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()