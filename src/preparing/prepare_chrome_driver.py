def prepare_chrome_driver():
    """
    Chromeのバージョンアップは頻繁に発生し、Webdriverとのバージョン不一致が多発するため、
    ChromeDriverManagerを使用し、自動的にバージョンを一致させる。
    selenium / webdriver-manager は呼び出し時に lazy import する（CI 環境等で未インストールの場合に
    モジュール読込で失敗しないよう）。
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_window_size(50, 50)
    return driver
