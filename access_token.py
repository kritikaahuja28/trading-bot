# -*- coding: utf-8 -*-

from kiteconnect import KiteConnect
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pyotp
import time


def autologin():
    with open("api_key.txt", 'r') as f:
        key_secret = f.read().strip().split()

    api_key = key_secret[0]
    api_secret = key_secret[1]
    user_id = key_secret[2]
    password = key_secret[3]
    totp_secret = key_secret[4]

    print("🔑 Credentials loaded successfully.")
    print(f"API Key: {api_key}, User ID: {user_id}")

    kite = KiteConnect(api_key=api_key)
    print(kite.login_url())

    service = ChromeService(executable_path="/opt/homebrew/bin/chromedriver")
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')  # use this only if you're not debugging visually
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--incognito')

    driver = webdriver.Chrome(service=service, options=options)

    try:
        print("🔗 Opening Kite login...")
        print("KITE LOGIN URL:", kite.login_url())
        driver.get(kite.login_url())
        print("DRIVER URL:", driver.current_url)
        wait = WebDriverWait(driver, 30)

        # Step 1: User ID
        user_field = wait.until(EC.presence_of_element_located((By.XPATH, '//input[@type="text"]')))
        user_field.send_keys(user_id)

        # Step 2: Password
        pass_field = wait.until(EC.presence_of_element_located((By.XPATH, '//input[@type="password"]')))
        pass_field.send_keys(password)

        driver.find_element(By.XPATH, '//button[@type="submit"]').click()

        # Step 3: TOTP (OTP input is now <input type="tel">)

        input_found = False
                # Step 1: Locate OTP input field using fallback logic
        print("⏳ Locating OTP input field...")

        otp_input = None
        for i in range(30):
            try:
                otp_input = driver.find_element(By.XPATH, '//input[@type="tel"]')
                print("🔍 OTP input field found as tel input.")
                break
            except:
                try:
                    otp_input = driver.find_element(By.XPATH, '//input[@type="text"]')
                    print("🔍 OTP input field found as text input.")
                    break
                except:
                    time.sleep(1)

        if not otp_input:
            driver.save_screenshot("otp_not_found.png")
            raise Exception("❌ OTP field not found. Screenshot saved.")

        # Step 2: Enter TOTP and submit
        totp = pyotp.TOTP(totp_secret).now()
        print(f"🔐 Entering TOTP: {totp}")
        time.sleep(2)
        otp_input.send_keys(totp)
        time.sleep(2)  # Wait for TOTP to be entered
        current_url = driver.current_url
        print("🔗 Current URL after entering TOTP:", current_url)
        # driver.find_element(By.XPATH, '//button[@type="submit"]').click()


        # # Final step: capture redirected URL
        # # time.sleep(5)
        # current_url = driver.current_url
        print("🔁 Redirected URL:", current_url)

        if "request_token=" not in current_url:
            if "captcha" in driver.page_source.lower():
                raise Exception("🚫 CAPTCHA detected. Cannot continue automatically.")
            raise Exception("❌ Login failed. Possibly invalid credentials or TOTP.")
        print("✅ Login successful. Capturing request token...")
        request_token = current_url.split("request_token=")[1].split("&")[0]
        print("Request Token:", request_token)
        # with open("request_token.txt", 'w') as f:
        #     f.write(request_token)

        print("✅ Request token saved.")

    finally:
        driver.quit()

    return request_token


def generate_access_token():
    request_token = autologin()
    print("Request Token:", request_token)
    with open("api_key.txt", 'r') as f:
        key_secret = f.read().strip().split()

    api_key = key_secret[0]
    api_secret = key_secret[1]

    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token=request_token, api_secret=api_secret)
    access_token = data["access_token"]

    # with open("access_token.txt", 'w') as f:
    #     f.write(access_token)
    print("Access Token:", access_token)
    print("✅ Access token saved.")
    return access_token


def get_access_token():
    access_token = None
    try:
        access_token = generate_access_token()
        # with open("access_token.txt", 'r') as f:
        #     access_token = f.read().strip()
        print("✅ Access token loaded from file.")
    except Exception as e:
        print("❌ Error loading access token:", e)
        print("❌ Access token file not found or inaccessible. Generating new access token...")
        # access_token = generate_access_token()

    return access_token

# request_token = autologin()

# access_token=get_access_token()