from __future__ import annotations

import json
from pathlib import Path

from app.domain.models import Vacancy
from app.storage.db import get_connection


class VacancyService:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def load_vacancies(self) -> list[Vacancy]:
        with get_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vacancies")
            rows = cursor.fetchall()
            return [Vacancy.from_dict(dict(row)) for row in rows]

    def save_vacancies(self, vacancies: list[dict]) -> None:
        with get_connection(self.db_path) as conn:
            cursor = conn.cursor()
            for vac in vacancies:
                cursor.execute(
                    "INSERT OR IGNORE INTO vacancies "
                    "(vacancy_id, title, company, location, url, description, "
                    "salary_from, salary_to, posted_date, skills) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        vac['id'],
                        vac['title'],
                        vac['company'],
                        vac['location'],
                        vac['url'],
                        vac['description'],
                        vac.get('salary_from'),
                        vac.get('salary_to'),
                        vac['posted_date'],
                        json.dumps(vac.get("skills", []), ensure_ascii=False),
                    )
                )
            conn.commit()
