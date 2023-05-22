I hate the fact that spotify won't let you filter by genre, so I made a little tool that builds a local database of your last.fm and spotify data, scrapes genres from RateYourMusic, and then gives you a (currently very buggy) UI that finally lets you filter your music by genre.

Needs https://github.com/dbeley/rymscraper

You will also need lastfm and spotify API keys, add them to the keys.json (and get rid of the '\_template' part)

A scraperAPI key is optional, but will help a good deal for getting RYM genres. You'll get 5000 free API calls with a new account (which should be enough for most people) and then 1000 API calls a month after that. If using scraperAPI just set your API key in keys.json and set `"use": "yes"`, otherwise it'll use rymscraper which is rather slow to avoid rate limiting from RYM.

Run `pip install -r requirements.txt` for dependencies

Run musiclib.py to set up the database and update it, run main.py for GUI (for now..). It will typically update once a day, but can be forced to update with the `--update` tag

Setting up the whole thing takes quite a while if you have a lot of scrobbles/saved albums so you'll probably wanna run it and forget about it for a day or so (assuming it doesn't break..) but it should set things up correctly even if you abort it several times in the process. It's really only meant for albums so I try to have it remove singles but it's tricky (is an hour-long song a single?) so I err on the side of caution, so you'll likely want to go through the database and clean stuff up yourself. Last.fm data isn't clean (And neither is Spotify's to an extent, so it can't be helped, though I do try to have it do a bit of cleaning.

