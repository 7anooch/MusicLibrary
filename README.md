I hate the fact that spotify won't let you filter by genre, so I made a little tool that builds a local database of your last.fm and spotify data, scrapes genres from RateYourMusic, and then gives you a (currently very buggy) UI that finally lets you filter your music by genre.

Needs https://github.com/dbeley/rymscraper

You will also need lastfm and spotify API keys, add them to the keys.json (and get rid of the '\_template' part)

A scraperAPI key is optional, but will help a good deal for getting RYM genres. You'll get 5000 free API calls with a new account (which should be enough for most people) and then 1000 API calls a month after that. If using scraperAPI just set your API key in keys.json and set `"use": "yes"`, otherwise it'll use rymscraper which is rather slow to avoid rate limiting from RYM.

Run `pip install -r requirements.txt` for dependencies

Run musiclib.py to set up the database and update it, run main.py for GUI (for now..)

Setting up the whole thing takes quite a while if you have a lot of scrobbles/saved albums.

