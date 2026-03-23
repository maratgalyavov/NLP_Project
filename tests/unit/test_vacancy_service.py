from datetime import date

from app.services.vacancy_service import VacancyService
from app.storage.db import init_db


def test_save_vacancies_falls_back_to_today_when_posted_date_missing(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    init_db(str(db_path))
    service = VacancyService(str(db_path))

    service.save_vacancies(
        [
            {
                "id": "vac_1",
                "title": "Python Developer",
                "company": "Acme",
                "location": "Moscow",
                "url": "https://example.com/vacancy/1",
                "description": "Build things",
                "salary_from": 100000,
                "salary_to": 150000,
                "posted_date": None,
                "skills": ["Python"],
                "active_flg": True,
            }
        ]
    )

    vacancies = service.load_vacancies()

    assert len(vacancies) == 1
    assert vacancies[0].posted_date == date.today().isoformat()
