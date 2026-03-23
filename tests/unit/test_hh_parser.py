from app.storage.hh_parser import ListingItem, _interleave_listing_batches, parse_search_page


def test_parse_search_page_collects_standard_cards() -> None:
    html = """
    <div data-qa="vacancy-serp__vacancy vacancy-serp-item_clickme">
      <a data-qa="serp-item__title" href="/vacancy/123456789?from=search">
        <span data-qa="serp-item__title-text">Python Developer</span>
      </a>
      <span data-qa="vacancy-serp__vacancy-employer-text">Acme</span>
      <span data-qa="vacancy-serp__vacancy-address">Москва</span>
    </div>
    """

    items = parse_search_page(html)

    assert len(items) == 1
    assert items[0].vacancy_id == "123456789"
    assert items[0].title == "Python Developer"
    assert items[0].company == "Acme"
    assert items[0].location == "Москва"


def test_parse_search_page_falls_back_to_plain_links() -> None:
    html = """
    <html>
      <body>
        <a href="/vacancy/987654321?query=python">Backend Engineer</a>
      </body>
    </html>
    """

    items = parse_search_page(html)

    assert len(items) == 1
    assert items[0].vacancy_id == "987654321"
    assert items[0].title == "Backend Engineer"


def test_interleave_listing_batches_balances_queries() -> None:
    batches = {
        "python": [
            ListingItem("1", "Python 1", "", "", "u1"),
            ListingItem("2", "Python 2", "", "", "u2"),
            ListingItem("3", "Python 3", "", "", "u3"),
        ],
        "frontend": [
            ListingItem("4", "Frontend 1", "", "", "u4"),
            ListingItem("5", "Frontend 2", "", "", "u5"),
        ],
        "qa": [
            ListingItem("6", "QA 1", "", "", "u6"),
        ],
    }

    items = _interleave_listing_batches(batches, max_vacancies=5)

    assert [item.vacancy_id for item in items] == ["1", "4", "6", "2", "5"]
