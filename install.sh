#/bin/sh

git clone https://github.com/dbeley/rymscraper.git
cd rymscraper
pip install -r requirements.txt
python setup.py install
cd ..
cd MusicLibrary
pip install -r requirements.txt
cp keys_template.json keys.json
