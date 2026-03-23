from __future__ import annotations

import datetime
import logging
import threading
from typing import Any

from app.core.config import settings
from app.services.vacancy_service import VacancyService
from app.storage.db import get_connection
from app.storage.hh_parser import DEFAULT_QUERIES, run

logger = logging.getLogger(__name__)


class ParserService:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.vacancy_service = VacancyService(db_path)
        self._lock = threading.Lock()
        self._status: dict[str, Any] = {
            "status": "idle",
            "message": "Parser has not been started yet",
            "stage": "idle",
            "started_at": None,
            "finished_at": None,
            "queries_used": 0,
            "pages_per_query": 0,
            "search_period_days": 0,
            "max_vacancies": self._max_vacancies(),
            "listings_collected": 0,
            "details_total": 0,
            "details_processed": 0,
            "vacancies_saved": 0,
            "vacancies_total": len(self.vacancy_service.load_vacancies()),
            "error": None,
        }

    @staticmethod
    def _now_iso() -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def _update_status(self, **updates: Any) -> None:
        with self._lock:
            self._status.update(updates)

    def _progress_update(self, payload: dict[str, Any]) -> None:
        stage = payload.get("stage", "running")
        message = "Parser is running"
        if stage == "details":
            processed = payload.get("details_processed", 0)
            total = payload.get("details_total", 0)
            message = f"Fetching vacancy details: {processed}/{total}"
        elif stage == "completed":
            message = "Parser run completed"
        self._update_status(
            stage=stage,
            message=message,
            listings_collected=payload.get("listings_collected", self.get_status().get("listings_collected", 0)),
            details_total=payload.get("details_total", self.get_status().get("details_total", 0)),
            details_processed=payload.get("details_processed", self.get_status().get("details_processed", 0)),
        )

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._status)


    def get_existing_vacancy_ids(self) -> set[str]:
        with get_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT vacancy_id FROM vacancies")
            return {row[0] for row in cursor.fetchall()}

    @staticmethod
    def _queries() -> list[str]:
        raw = settings.parser_queries_raw.strip()
        if not raw:
            return DEFAULT_QUERIES
        parts = [item.strip() for item in raw.replace("\n", "|").split("|")]
        return [item for item in parts if item]

    @staticmethod
    def _max_vacancies() -> int | None:
        return settings.parser_max_vacancies or None

    def parse_and_store_vacancies(
        self,
        queries: list[str],
        area: str = "1",
        pages: int = 1,
        *,
        progress_callback: Any | None = None,
    ) -> int:
        logger.info(
            "Starting parser run: queries=%s area=%s pages=%s search_period_days=%s max_vacancies=%s delay=%s",
            len(queries),
            area,
            pages,
            settings.parser_search_period_days,
            self._max_vacancies(),
            settings.parser_delay_seconds,
        )
        vacancies = run(
            queries=queries,
            area=area,
            pages_per_query=pages,
            delay=settings.parser_delay_seconds,
            max_vacancies=self._max_vacancies(),
            order_by="publication_time",
            search_period=settings.parser_search_period_days,
            progress_callback=progress_callback,
        )

        self.vacancy_service.save_vacancies(vacancies)
        logger.info("Parser run completed: vacancies_saved=%s", len(vacancies))
        return len(vacancies)

    def daily_update(self) -> None:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        logger.info(
            "Starting daily parser update: queries=%s area=%s pages=%s search_period_days=%s max_vacancies=%s",
            len(self._queries()),
            settings.parser_area,
            settings.parser_daily_pages_per_query,
            settings.parser_daily_search_period_days,
            self._max_vacancies(),
        )
        vacancies = run(
            queries=self._queries(),
            area=settings.parser_area,
            pages_per_query=settings.parser_daily_pages_per_query,
            delay=settings.parser_delay_seconds,
            max_vacancies=self._max_vacancies(),
            order_by="publication_time",
            search_period=settings.parser_daily_search_period_days,
            posted_since=yesterday,
            skip_if_no_posted_date=True,
        )

        if vacancies:
            self.vacancy_service.save_vacancies(vacancies)
        logger.info("Daily parser update completed: vacancies_saved=%s", len(vacancies))

    def _run_parser_job(self, queries: list[str], area: str, pages: int) -> None:
        try:
            parsed_count = self.parse_and_store_vacancies(
                queries=queries,
                area=area,
                pages=pages,
                progress_callback=self._progress_update,
            )
        except Exception as exc:
            logger.exception("Parser run failed")
            self._update_status(
                status="failed",
                stage="failed",
                message="Parser run failed",
                finished_at=self._now_iso(),
                error=str(exc),
            )
            return

        self._update_status(
            status="completed",
            stage="completed",
            message="Parser run completed",
            finished_at=self._now_iso(),
            vacancies_saved=parsed_count,
            vacancies_total=len(self.vacancy_service.load_vacancies()),
            error=None,
        )

    def run_parser(self) -> dict[str, Any]:
        current = self.get_status()
        if current.get("status") == "running":
            current["message"] = "Parser run is already in progress"
            return current

        queries = self._queries()
        area = settings.parser_area
        pages = settings.parser_pages_per_query
        self._update_status(
            status="running",
            stage="search",
            message="Collecting vacancy listings",
            started_at=self._now_iso(),
            finished_at=None,
            queries_used=len(queries),
            pages_per_query=pages,
            search_period_days=settings.parser_search_period_days,
            max_vacancies=self._max_vacancies(),
            listings_collected=0,
            details_total=0,
            details_processed=0,
            vacancies_saved=0,
            error=None,
        )
        thread = threading.Thread(
            target=self._run_parser_job,
            args=(queries, area, pages),
            daemon=True,
            name="parser-run",
        )
        thread.start()
        return self.get_status()
