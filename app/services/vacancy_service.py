from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from app.domain.models import Vacancy


class VacancyService:
    def __init__(self, vacancies_path: str) -> None:
        self.vacancies_path = vacancies_path

    def load_vacancies(self) -> list[Vacancy]:
        raw = Path(self.vacancies_path).read_text(encoding="utf-8")
        payload = json.loads(raw)
        return [Vacancy.from_dict(item) for item in payload]

    def get_vacancy(self, vacancy_id: str) -> Optional[Vacancy]:
        for vac in self.load_vacancies():
            if vac.id == vacancy_id:
                return vac
        return None
