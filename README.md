# X_scrapper

## Overview

This project is a Twitter scraping tool designed to extract tweets, retweets, replies, and mutual followership information for specified users. The tool does not use Tweepy and relies on other Python libraries to perform its tasks such as **twscrape**.

## Project Structure

- `scrapper.py`: The main script that extracts Twitter data.
- `requirements.txt`: Lists the Python libraries required to run the script.
- `credentials.txt`: Contains Twitter user credentials.
- `users.rtf`: A file containing the usernames of the Twitter accounts to be scraped.

## Prerequisites

Ensure you have Python 3.10 installed on your system. You can download it from [python.org](https://www.python.org/downloads/).

## Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-username/X_scrapper.git
   cd twitter-scraping-tool
   pip install -r requirements.txt

2. **Setup user credentials:**
The format of the user credentials should be as follows:
    ```bash
    username:password:email:password
Note : You can add multiple accounts.

3. **Execute:**
In order to execute the script run :
    ```bash
    python3 scrapper.py credentials.txt users.rtf <Text limit eg.=20> <saving_path>

4. **Limitations:**
Possible timeout requests - Account banning!

If you find this tool useful, don't forget to drop as star !  <3

