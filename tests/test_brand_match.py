from producers.common import Brand, match_brand


def test_match_brand_word_boundary():
    brands = [
        Brand(name="acme", keywords=("acme", "acme corp"), subreddits=("all",)),
        Brand(name="globex", keywords=("globex",), subreddits=("all",)),
    ]
    assert match_brand("I had a problem with Acme yesterday.", brands) == "acme"
    assert match_brand("globex is failing again", brands) == "globex"
    # substring match must NOT trigger
    assert match_brand("acmedical is fine", brands) is None
    assert match_brand("nothing relevant here", brands) is None
