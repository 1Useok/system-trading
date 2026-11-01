import os
import json
import yaml
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
import aiohttp

logger = logging.getLogger(__name__)


class KISTokenManager:
    """한국투자증권 API 토큰 관리 클래스 (SVR 기반 vps/prod 이중 설정 지원)"""
    
    _vps_instance = None   # 모의투자용 인스턴스 (SVR=vps)
    _prod_instance = None  # 실전투자용 인스턴스 (SVR=prod)
    _lock = asyncio.Lock()
    
    def __new__(cls, svr: str = 'vps'):
        """
        TokenManager 인스턴스 생성
        
        Args:
            svr: 'vps' (모의투자), 'prod' (실전투자)
        """
        if svr == 'vps':
            if cls._vps_instance is None:
                cls._vps_instance = super().__new__(cls)
            return cls._vps_instance
        elif svr == 'prod':
            if cls._prod_instance is None:
                cls._prod_instance = super().__new__(cls)
            return cls._prod_instance
        else:
            raise ValueError(f"Invalid svr: {svr}. Use 'vps' or 'prod'")
    
    def __init__(self, svr: str = 'vps'):
        # 이미 초기화된 경우 건너뛰기
        instance_attr = f'_initialized_{svr}'
        if hasattr(self, instance_attr):
            return
            
        setattr(self, instance_attr, True)
        self.svr = svr
        self.access_token: Optional[str] = None
        self.token_expired: Optional[datetime] = None
        self.app_key: Optional[str] = None
        self.app_secret: Optional[str] = None
        self.is_paper: bool = False
        
        # 토큰 저장 경로 설정
        self.token_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".tokens")
        os.makedirs(self.token_dir, exist_ok=True)
        
        # 환경별 토큰 파일명
        self._token_file: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None
        
        # KIS API 엔드포인트 (SVR 기반)
        self.TOKEN_ENDPOINTS = {
            "prod": "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
            "vps": "https://openapivts.koreainvestment.com:29443/oauth2/tokenP"
        }
    
    def configure(self, config: Dict):
        """API 키와 설정 정보 구성"""
        self.app_key = config.get('app_key') or os.getenv("KIS_APP_KEY")
        self.app_secret = config.get('app_secret') or os.getenv("KIS_APP_SECRET")
        self.is_paper = config.get('is_dev', False)
        
        # 환경에 따른 토큰 파일 설정
        today = datetime.today().strftime("%Y%m%d")
        self._token_file = os.path.join(self.token_dir, f"kis_token_{self.svr}_{today}.yaml")
        
        logger.info(f"TokenManager configured: svr={self.svr}, paper_trading={self.is_paper}")
    
    @classmethod
    def get_vps_instance(cls):
        """모의투자용 TokenManager 인스턴스 반환 (SVR=vps)"""
        return cls(svr='vps')
    
    @classmethod
    def get_prod_instance(cls):
        """실전투자용 TokenManager 인스턴스 반환 (SVR=prod)"""
        return cls(svr='prod')
    
    @classmethod
    def get_instance_by_context(cls, context: str):
        """
        컨텍스트에 따른 TokenManager 인스턴스 반환
        
        Args:
            context: 'order', 'balance' -> vps, 그외 -> prod
        """
        if context in ['order', 'balance']:
            return cls.get_vps_instance()
        else:
            return cls.get_prod_instance()
    
    async def get_token(self) -> str:
        """유효한 토큰 반환 (필요시 자동 갱신)"""
        async with self._lock:
            # 토큰이 유효한지 확인
            if self._is_token_valid():
                return self.access_token
            
            # 저장된 토큰 로드 시도
            if self._load_token_from_file():
                if self._is_token_valid():
                    logger.info("Loaded valid token from file")
                    return self.access_token
            
            # 새 토큰 발급
            await self._request_new_token()
            return self.access_token
    
    def _is_token_valid(self) -> bool:
        """토큰 유효성 검사"""
        if not self.access_token or not self.token_expired:
            return False
        
        # 만료 10분 전에 갱신
        buffer_time = datetime.now() + timedelta(minutes=10)
        return self.token_expired > buffer_time
    
    def _load_token_from_file(self) -> bool:
        """파일에서 토큰 로드"""
        try:
            if not os.path.exists(self._token_file):
                return False
                
            with open(self._token_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                
            self.access_token = data.get('token')
            expired_str = data.get('valid_date')
            
            if expired_str:
                self.token_expired = datetime.strptime(expired_str, "%Y-%m-%d %H:%M:%S")
                logger.info(f"Token loaded from file, expires at: {self.token_expired}")
                return True
                
        except Exception as e:
            logger.warning(f"Failed to load token from file: {e}")
            
        return False
    
    def _save_token_to_file(self):
        """토큰을 파일에 저장"""
        try:
            data = {
                'token': self.access_token,
                'valid_date': self.token_expired.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            with open(self._token_file, 'w', encoding='utf-8') as f:
                yaml.dump(data, f)
                
            logger.info(f"Token saved to file: {self._token_file}")
            
        except Exception as e:
            logger.error(f"Failed to save token to file: {e}")
    
    async def _request_new_token(self):
        """KIS API에서 새 토큰 발급"""
        if not self._session:
            self._session = aiohttp.ClientSession()
        
        endpoint = self.TOKEN_ENDPOINTS[self.svr]
        
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8"
        }
        
        logger.info(f"Requesting new token from {endpoint}")
        
        try:
            async with self._session.post(endpoint, json=data, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Token request failed: {response.status} - {error_text}")
                
                result = await response.json()
                
                self.access_token = result.get("access_token")
                expired_str = result.get("access_token_token_expired")
                
                if not self.access_token or not expired_str:
                    raise Exception("Invalid token response")
                
                self.token_expired = datetime.strptime(expired_str, "%Y-%m-%d %H:%M:%S")
                
                # 토큰 저장
                self._save_token_to_file()
                
                logger.info(f"New token acquired, expires at: {self.token_expired}")
                
        except Exception as e:
            logger.error(f"Failed to request new token: {e}")
            raise
    
    async def get_approval_key(self) -> str:
        """WebSocket용 실시간 접속키 발급"""
        if not self._session:
            self._session = aiohttp.ClientSession()
        
        # 실시간 접속키는 별도로 관리 (24시간 유효)
        if hasattr(self, '_approval_key') and hasattr(self, '_approval_key_expired'):
            if self._approval_key and datetime.now() < self._approval_key_expired:
                return self._approval_key
        
        endpoint = self.TOKEN_ENDPOINTS[self.svr]
        approval_url = endpoint.replace("/tokenP", "").replace("/oauth2", "") + "/oauth2/Approval"
        
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret
        }
        
        headers = {
            "Content-Type": "application/json; charset=UTF-8"
        }
        
        logger.info(f"Requesting approval key from {approval_url}")
        
        try:
            async with self._session.post(approval_url, json=data, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Approval key request failed: {response.status} - {error_text}")
                
                result = await response.json()
                self._approval_key = result.get("approval_key")
                
                if not self._approval_key:
                    raise Exception("Invalid approval key response")
                
                # 접속키는 24시간 유효
                self._approval_key_expired = datetime.now() + timedelta(hours=24)
                
                logger.info(f"Approval key acquired, expires at: {self._approval_key_expired}")
                return self._approval_key
                
        except Exception as e:
            logger.error(f"Failed to request approval key: {e}")
            raise
    
    async def close(self):
        """리소스 정리"""
        if self._session:
            await self._session.close()
            self._session = None