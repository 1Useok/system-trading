# -*- coding: utf-8 -*-
"""
통합 설정 로더

패키지 내부 `config.yaml`(TR_ID별 엔드포인트, 실행 라우팅, 구독 종목)과
`.env`(API 키, 계좌번호)를 읽어 하나의 설정 dict로 병합합니다.

- `vps_api`  : 모의투자 서버. 주문 / 잔고 / 체결통보에 사용
- `prod_api` : 실전 서버. 실시간 선물 시세(WebSocket, 실전 전용 TR_ID)와 데이터 조회에 사용
"""
import os
import yaml
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Compatibility helper to open package resource across Python 3.8+ without external backports
try:
    from importlib.resources import files as _files

    def _open_package_config():
        return _files("trading_system").joinpath("config").joinpath("config.yaml").open("r", encoding="utf-8")
except Exception:
    import pkgutil, io

    def _open_package_config():
        data = pkgutil.get_data("trading_system", "config/config.yaml")
        if data is None:
            raise FileNotFoundError("Package resource not found: trading_system/config/config.yaml")
        return io.StringIO(data.decode("utf-8"))

logger = logging.getLogger(__name__)


class ConfigLoader:
    """config.yaml + .env 병합 설정 로더."""

    def __init__(self, config_path: Optional[str] = None, env_path: str = ".env"):
        """
        Args:
            config_path: config.yaml 파일 경로 (None이면 패키지 내부 리소스를 사용)
            env_path: .env 파일 경로
        """
        self.config_path = config_path
        self.env_path = env_path
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """설정 파일과 .env 파일을 로드하여 병합합니다."""
        if os.path.exists(self.env_path):
            load_dotenv(dotenv_path=self.env_path)
            logger.info(f"환경 변수 파일(.env) 로드 완료: {self.env_path}")
        else:
            logger.warning(f"환경 변수 파일을 찾을 수 없습니다: {self.env_path}")

        try:
            if self.config_path is None:
                with _open_package_config() as f:
                    config = yaml.safe_load(f)
            else:
                if not os.path.exists(self.config_path):
                    raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {self.config_path}")
                with open(self.config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
        except Exception as e:
            logger.error(
                f"설정 파일 로드 중 오류 발생 {self.config_path or 'package resource trading_system/config/config.yaml'}: {e}",
                exc_info=True,
            )
            raise

        # 모의투자 계좌번호: VPS_KIS_ACCOUNT_NO 우선, 없으면 KIS_ACCOUNT_NO (형식: "12345678-03")
        vps_account = os.getenv("VPS_KIS_ACCOUNT_NO") or os.getenv("KIS_ACCOUNT_NO", "")

        config["vps_api"] = {
            "app_key": os.getenv("VPS_KIS_APP_KEY", ""),
            "app_secret": os.getenv("VPS_KIS_APP_SECRET", ""),
            "account_no": vps_account.strip(),
            "base_url": os.getenv("VPS_BASE_URL", "https://openapivts.koreainvestment.com:29443"),
            # 체결통보(H0IFCNI0) WebSocket 구독에 필요한 KIS HTS 아이디.
            "hts_id": os.getenv("KIS_HTS_ID", ""),
            "is_dev": True,
        }

        config["prod_api"] = {
            "app_key": os.getenv("PROD_KIS_APP_KEY", ""),
            "app_secret": os.getenv("PROD_KIS_APP_SECRET", ""),
            "account_no": os.getenv("PROD_KIS_ACCOUNT_NO", "").strip(),
            "base_url": os.getenv("PROD_BASE_URL", "https://openapi.koreainvestment.com:9443"),
            "hts_id": os.getenv("KIS_HTS_ID", ""),
            "is_dev": False,
        }

        logger.info("설정 파일과 환경 변수 병합 완료.")
        return config

    def get_vps_config(self) -> Dict[str, Any]:
        """VPS(모의투자) API 설정을 반환합니다."""
        return self._config.get("vps_api", {})

    def get_prod_config(self) -> Dict[str, Any]:
        """PROD(실전투자) API 설정을 반환합니다."""
        return self._config.get("prod_api", {})

    def get_config_by_svr(self, svr: str) -> Dict[str, Any]:
        """SVR('vps' | 'prod')에 따른 API 설정을 반환합니다."""
        if svr == "vps":
            return self.get_vps_config()
        elif svr == "prod":
            return self.get_prod_config()
        raise ValueError(f"Invalid svr: {svr}. Use 'vps' or 'prod'")

    def get_config(self) -> Dict[str, Any]:
        """전체 설정 객체를 반환합니다."""
        return self._config

    def get_api_endpoints(self) -> Dict[str, str]:
        """API 엔드포인트 설정을 반환합니다."""
        return self._config.get("kis_api_endpoints", {})
