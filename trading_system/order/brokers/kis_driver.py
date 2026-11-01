from trading_system.api.kis.rest_api import RestApi

class KISDriver:
    def __init__(self, config):
        # 주문/잔고 전용이므로 'order' 컨텍스트 사용 (VPS 모의투자)
        self.client = RestApi(config, context='order')

    ## STOCK
    async def order_stock(self, **kwargs):
        return await self.client.order_stock(**kwargs)

    async def cancel_stock_order(self, **kwargs):
        return await self.client.cancel_stock_order(**kwargs)

    async def get_stock_balance(self, **kwargs):
        return await self.client.get_stock_balance(**kwargs)

    async def get_stock_orders(self, **kwargs):
        return await self.client.get_stock_orders(**kwargs)

    async def get_stock_price(self, **kwargs):
        return await self.client.get_stock_price(**kwargs)

    ## FUTURE
    async def order_future(self, **kwargs):
        return await self.client.order_future(**kwargs)

    async def cancel_future_order(self, *, account, order_id=None, client_order_id=None, symbol="", quantity=0, **kwargs):
        # KIS 취소는 브로커 주문번호(원주문번호)로만 가능하다. client_order_id는 KIS가 모른다.
        if not order_id:
            raise ValueError("KIS cancel requires broker order_id (ODNO)")
        return await self.client.cancel_future_order(
            account=account, original_order_no=order_id, symbol=symbol, quantity=quantity,
        )

    async def get_future_account_balance(self, **kwargs):
        return await self.client.get_future_account_balance(**kwargs)

    async def get_future_orders(self, **kwargs):
        return await self.client.get_future_orders(**kwargs)

    async def get_future_price(self, **kwargs):
        return await self.client.get_future_price(**kwargs)
