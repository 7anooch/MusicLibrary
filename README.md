I hate the fact that spotify won't let you filter by genre, so I made a little tool that builds a local database of your last.fm and spotify data, scrapes genres from RateYourMusic, and then gives you a (currently very buggy UI) that finally lets you filter your music by genre.

needs https://github.com/dbeley/rymscraper
you will also need lastfm and spotify API keys, add them to the keys.json (and get rid of the '\_template' part)

run musiclib.py to set up the database and update it, run main.py for GUI
