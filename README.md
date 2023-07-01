I hate the fact that spotify won't let you filter by genre, so I made a little tool that builds a local database of your last.fm and spotify data, scrapes genres from RateYourMusic, and then gives you a (currently very buggy) UI that finally lets you filter your music by genre.

![image](https://github.com/7anooch/MusicLibrary/assets/129010989/4306824f-9e66-4c95-a706-8cee3ccfd5b0)


# Installation Instructions for Beginners on Windows
*on mac/linux just skip steps 1 and 2, and instead of Git Bash you want to use your terminal*

For people unfamiliar with how to use a command line interface, 3 commands are useful: `pwd` (**p**rint **w**orking **d**irectory) tells you which directory (folder) you're in, `ls` lists the files in the directory, and `cd` allows you to **c**hange **d**irectories. So you can navigate between directories with commands like `cd Documents` or `cd ..` to go up a directory.

**Step 1: Install Python:** https://www.python.org/downloads/windows/

- In the installer, make sure to check the box that says "Add Python to PATH" before clicking Install.

**Step 2: Install Git:** https://gitforwindows.org/

- Make sure to keep the "Git Bash Here" option checked in the installer.

**Step 3: Download the Software**

- Navigate to the folder you want to put the software in, then right click and select “git bash here”
- enter the command: `./install.sh`
- Assuming all goes well, it will install a bunch of software. It may ask you to install Visual C++ Build Tools, I’m not sure why. If its unable to install a package, you might be able to find the package directly at https://www.lfd.uci.edu/~gohlke/pythonlibs/ and then install it by using `pip install [package you just downloaded]`

**Step 4: Get API Keys**

- For the Spotify API key, go to the Spotify Developer Dashboard and log in or create an account: https://developer.spotify.com/dashboard/
- Click on "Create an App" and fill in the necessary information.
- Once the app is created, you will be able to see the Client ID and Client Secret. These are your API keys. It also lets you set a callback url, set it to http://localhost:8080/callback
- For the Last.fm API key, go to the Last.fm API account creation page and fill in the necessary information: https://www.last.fm/api/account/create
- After submitting the form, you will be provided with an API key.
- You should now have a file in MusicLibrary called keys.json. If you don’t, enter the command `cp keys_template.json keys.json` to create it
- Open keys.json in a text editor and enter your API keys. Replace "spotify_id" and "spotify_secret" with the Client ID and Client Secret from Spotify, respectively. Replace "lastfm_key" with the API key from Last.fm. Make sure to keep the quotes around the keys.
- A scraperAPI key is not necessary, but can be helpful. You do not need a discogs API key at this time. Look at the section below that and see if you want to change any parameters.

You’re all set! Now you can use `python musiclib.py` to start setting up the database, which can take a day or two. You can also use `python main.py`to start the GUI. I recommend running both commands in separate tabs so you can use the GUI while the database setup is still running. When you run `python musiclib.py` after the initial run, it will open up a browser tab to authenticate your spotify api key. It will first go to a spotify link and then redirect to a link that begins with `localhost`, copy that link and paste it in the command line when it asks you for a redirected  link. That’s all!

A scraperAPI key is optional, but can help a good deal for getting RYM genres. You'll get 5000 free API calls with a new account (which should be enough for most people) and then 1000 API calls a month after that. If using scraperAPI just set your API key in keys.json and set `"use": "yes"`, otherwise it'll use rymscraper which is rather slow to avoid rate limiting from RYM.

Run `python musiclib.py` to set up the database and update it, run `python main.py` for GUI (for now..). The database will update once a day, but you can force it to update at anytime by running `python musiclib.py --update`

Setting up the whole thing takes quite a while if you have a lot of scrobbles/saved albums so you'll probably wanna run it and forget about it for a day or so (assuming it doesn't break..) but it should set things up correctly even if you abort it several times in the process. It's really only meant for albums so I try to have it remove singles but it's tricky (is an hour-long song a single?) so I err on the side of caution, so you'll likely want to go through the database and clean stuff up yourself. Last.fm data isn't clean (And neither is Spotify's to an extent) so it can't be helped, though I do try to have it do a bit of cleaning.


