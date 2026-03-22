from __future__ import annotations

import datetime
import json
import time
from typing import Any, Dict, List

from app.storage.db import get_connection
from app.storage.hh_parser import run
from app.core.config import settings
from app.services.vacancy_service import VacancyService

DEFAULT_QUERIES = [
    "Python developer",
    "Java developer",
    "аналитик данных",
    "системный аналитик",
    "QA engineer",
    "тестировщик",
    "DevOps",
]

class ParserService:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.vacancy_service = VacancyService(db_path)

    def get_existing_vacancy_ids(self) -> set[str]:
        with get_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT vacancy_id FROM vacancies")
            return {row[0] for row in cursor.fetchall()}

    def parse_and_store_vacancies(self, queries: List[str], area: str = "1", pages: int = 1) -> None:
        # Get existing vacancies to avoid duplicates

        # Run parser
        vacancies = run(
            queries=queries,
            area=area,
            pages_per_query=pages,
            delay=0.6,
            max_vacancies=None,
            order_by="publication_time",
            search_period=1,
        )

        self.vacancy_service.save_vacancies(vacancies)

    def daily_update(self) -> None:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        
        # Run parser for yesterday's vacancies
        vacancies = run(
            queries=DEFAULT_QUERIES,
            area="1",
            pages_per_query=5,
            delay=0.6,
            max_vacancies=None,
            order_by="publication_time",
            search_period=1,
            posted_since=yesterday,
            skip_if_no_posted_date=True
        )

        # Save new vacancies
        if vacancies:
            self.vacancy_service.save_vacancies(vacancies)

    def run_parser(self) -> Dict[str, Any]:
        """Main entry point for the parser service"""

        self.parse_and_store_vacancies(
            queries=DEFAULT_QUERIES,
            area="113",
            pages=1
        )

        return {
            "status": "success",
            "message": "Vacancy parsing completed",
            "vacancies_parsed": len(self.vacancy_service.load_vacancies())
        }