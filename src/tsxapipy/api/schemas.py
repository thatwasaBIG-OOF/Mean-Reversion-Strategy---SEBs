# tsxapipy/api/schemas.py
"""
Pydantic models for API request payloads and response structures.
"""
from datetime import datetime
from typing import List, Optional, Any, Dict, Union, Literal # Literal is already imported
from pydantic import BaseModel, Field, validator, model_validator # validator is V1, field_validator for V2 field specific

# --- Common Base Models ---

class BaseRequestModel(BaseModel):
    """Base model for API requests to enforce common configurations."""
    class Config:
        populate_by_name = True 
        # extra = 'forbid' 

class BaseResponseModel(BaseModel):
    """Base model for API responses, assuming a common 'success' field."""
    success: bool
    error_message: Optional[str] = Field(None, alias="errorMessage")
    error_code: Optional[Any] = Field(None, alias="errorCode") 

    class Config:
        populate_by_name = True
        # extra = 'ignore' 


# --- Authentication Models ---

class AuthLoginKeyRequest(BaseRequestModel):
    user_name: str = Field(..., alias="userName")
    api_key: str = Field(..., alias="apiKey")

class AuthLoginAppRequest(BaseRequestModel):
    user_name: str = Field(..., alias="userName")
    password: str
    app_id: str = Field(..., alias="appId")
    verify_key: str = Field(..., alias="verifyKey")

class AuthResponse(BaseResponseModel):
    token: Optional[str] = None

class AuthValidateResponse(BaseResponseModel):
    new_token: Optional[str] = Field(None, alias="newToken")


# --- Account Models ---

class AccountSearchRequest(BaseRequestModel):
    only_active_accounts: bool = Field(True, alias="onlyActiveAccounts")

class Account(BaseModel): 
    id: int 
    name: Optional[str] = None
    balance: Optional[float] = None 
    can_trade: Optional[bool] = Field(None, alias="canTrade")
    is_visible: Optional[bool] = Field(None, alias="isVisible")
    
    class Config:
        populate_by_name = True

class AccountSearchResponse(BaseResponseModel):
    accounts: List[Account] = []


# --- Contract Models ---

class ContractSearchRequest(BaseRequestModel):
    search_text: str = Field(..., alias="searchText")
    live: bool = False

class ContractSearchByIdRequest(BaseRequestModel):
    contract_id: str = Field(..., alias="contractId")

class Contract(BaseModel): 
    id: str 
    name: Optional[str] = None
    description: Optional[str] = None
    tick_size: Optional[float] = Field(None, alias="tickSize")
    tick_value: Optional[float] = Field(None, alias="tickValue")
    currency: Optional[str] = None
    instrument_id: Optional[int] = Field(None, alias="instrumentId") 
    
    class Config:
        populate_by_name = True

class ContractSearchResponse(BaseResponseModel):
    contracts: List[Contract] = []


# --- Historical Data Models ---

class HistoricalBarsRequest(BaseRequestModel):
    account_id: Optional[int] = Field(None, alias="accountId") # <--- ADDED
    contract_id: Union[str, int] = Field(..., alias="contractId") 
    live: bool = False
    start_time: str = Field(..., alias="startTime") 
    end_time: str = Field(..., alias="endTime")   
    unit: int
    unit_number: int = Field(..., alias="unitNumber")
    limit: int
    include_partial_bar: bool = Field(False, alias="includePartialBar")

class BarData(BaseModel): 
    t: datetime 
    o: float    
    h: float    
    l: float    
    c: float    
    v: float    

    # For Pydantic V2, prefer @field_validator for specific fields if complex logic needed.
    # Pydantic V2 automatically tries to parse ISO strings to datetime for datetime fields.
    # This @validator might be redundant or could be more specific if needed.
    @validator('t', pre=True, allow_reuse=True) # allow_reuse=True is good practice
    def parse_timestamp(cls, value):
        if isinstance(value, str):
            return value # Pydantic will handle parsing
        # Can add more robust parsing here if API sometimes sends non-standard strings
        return value 

class HistoricalBarsResponse(BaseResponseModel):
    bars: List[BarData] = []


# --- Order Models ---
# ORDER_TYPES_MAP: 1=LIMIT, 2=MARKET, 3=STOP (Stop Market)
# ORDER_SIDES_MAP: 0=BUY, 1=SELL

class OrderBase(BaseRequestModel):
    account_id: int = Field(..., alias="accountId")
    contract_id: str = Field(..., alias="contractId")
    type: int # Actual order type code
    side: int # 0 for Buy, 1 for Sell
    size: int = Field(gt=0) 
    custom_tag: Optional[str] = Field(None, alias="customTag")
    linked_order_id: Optional[int] = Field(None, alias="linkedOrderld") # Alias for 'linkedOrderId'

class PlaceMarketOrderRequest(OrderBase):
    type: Literal[2] = 2 # Market order type is 2

class PlaceLimitOrderRequest(OrderBase):
    type: Literal[1] = 1 # Limit order type is 1
    limit_price: float = Field(..., alias="limitPrice", gt=0)

class PlaceStopOrderRequest(OrderBase): # Assuming this is Stop Market
    type: Literal[3] = 3 # Stop order type is 3 (assuming Stop Market)
    stop_price: float = Field(..., alias="stopPrice", gt=0)

# If TRAILING_STOP is type 5, and you want to support it directly:
class PlaceTrailingStopOrderRequest(OrderBase):
    type: Literal[5] = 5 # Trailing Stop order type is 5
    trail_price: float = Field(..., alias="trailPrice", gt=0) # Or trailOffset, check API docs carefully

class OrderPlacementResponse(BaseResponseModel):
    order_id: Optional[int] = Field(None, alias="orderId")

class CancelOrderRequest(BaseRequestModel):
    account_id: int = Field(..., alias="accountId")
    order_id: int = Field(..., alias="orderId")

class CancelOrderResponse(BaseResponseModel): 
    pass

class ModifyOrderRequest(BaseRequestModel):
    account_id: int = Field(..., alias="accountId")
    order_id: int = Field(..., alias="orderId") 
    size: Optional[int] = Field(None, gt=0)
    limit_price: Optional[float] = Field(None, alias="limitPrice", gt=0)
    stop_price: Optional[float] = Field(None, alias="stopPrice", gt=0)
    trail_price: Optional[float] = Field(None, alias="trailPrice", gt=0)
    
    @model_validator(mode='before')
    @classmethod # model_validator in Pydantic v2 should be a classmethod if mode='before'
    def check_at_least_one_modifiable_field(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        modifiable_fields = {'size', 'limit_price', 'stop_price', 'trail_price'}
        # Check if any of the modifiable fields are present and not None in the input data
        if not any(values.get(field) is not None for field in modifiable_fields):
            raise ValueError("At least one modifiable field (size, limitPrice, stopPrice, trailPrice) must be provided for order modification.")
        return values

class ModifyOrderResponse(BaseResponseModel): 
    pass

class OrderSearchRequest(BaseRequestModel):
    account_id: int = Field(..., alias="accountId")
    start_timestamp: str = Field(..., alias="startTimestamp") 
    end_timestamp: Optional[str] = Field(None, alias="endTimestamp") 

class OrderDetails(BaseModel): 
    id: int
    status: Optional[int] = None 
    contract_id: Optional[str] = Field(None, alias="contractId")
    type: Optional[int] = None
    side: Optional[int] = None
    size: Optional[int] = None
    cum_quantity: Optional[int] = Field(None, alias="cumQuantity")
    avg_px: Optional[float] = Field(None, alias="avgPx")
    limit_price: Optional[float] = Field(None, alias="limitPrice")
    stop_price: Optional[float] = Field(None, alias="stopPrice")
    leaves_quantity: Optional[int] = Field(None, alias="leavesQuantity")
    creation_timestamp: Optional[datetime] = Field(None, alias="creationTimestamp")
    update_timestamp: Optional[datetime] = Field(None, alias="updateTimestamp")
    
    class Config:
        populate_by_name = True

class OrderSearchResponse(BaseResponseModel):
    orders: List[OrderDetails] = []


# --- Position Models ---

class CloseContractPositionRequest(BaseRequestModel):
    account_id: int = Field(..., alias="accountId")
    contract_id: str = Field(..., alias="contractId")

class PartialCloseContractPositionRequest(CloseContractPositionRequest):
    size: int = Field(..., gt=0) 

class PositionManagementResponse(BaseResponseModel): 
    message: Optional[str] = None

class SearchOpenPositionsRequest(BaseRequestModel):
    account_id: int = Field(..., alias="accountId")

class Position(BaseModel): 
    account_id: Optional[int] = Field(None, alias="accountId") 
    contract_id: Optional[str] = Field(None, alias="contractId")
    size: Optional[int] = None
    average_price: Optional[float] = Field(None, alias="averagePrice")
    unrealized_pnl: Optional[float] = Field(None, alias="unrealizedPnl")
    
    class Config:
        populate_by_name = True

class SearchOpenPositionsResponse(BaseResponseModel):
    positions: List[Position] = []


# --- Trade Models ---

class TradeSearchRequest(BaseRequestModel):
    account_id: int = Field(..., alias="accountId")
    start_timestamp: str = Field(..., alias="startTimestamp") 
    end_timestamp: Optional[str] = Field(None, alias="endTimestamp") 

class Trade(BaseModel): 
    id: int 
    order_id: Optional[int] = Field(None, alias="orderId")
    contract_id: Optional[str] = Field(None, alias="contractId")
    creation_timestamp: Optional[datetime] = Field(None, alias="creationTimestamp")
    price: Optional[float] = None
    size: Optional[int] = None
    side: Optional[int] = None 
    profit_and_loss: Optional[float] = Field(None, alias="profitAndLoss")
    
    class Config:
        populate_by_name = True

class TradeSearchResponse(BaseResponseModel):
    trades: List[Trade] = []

# --- Generic Success/Failure Response (if needed for simple endpoints) ---
class GenericSuccessResponse(BaseResponseModel):
    message: Optional[str] = None