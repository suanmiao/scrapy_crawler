sudo apt update
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt-get install -f -y
google-chrome --version


wget https://chromedriver.storage.googleapis.com/92.0.4515.107/chromedriver_linux64.zip

unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/bin/chromedriver
sudo chown root:root /usr/bin/chromedriver
sudo chmod +x /usr/bin/chromedriver

# Please make sure your cluster has scrapy installed, otherwise use the command below
pip install scrapy
pip install selenium
pip install webdriver-manager
