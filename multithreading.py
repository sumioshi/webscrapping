import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup
from multiprocessing import Pool

# global headers to be used for requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

MAX_THREADS = 10
CSV_FILE = 'movies.csv'


def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = requests.get(movie_link, headers=headers)
    movie_soup = BeautifulSoup(response.content, 'html.parser')

    if movie_soup is not None:
        title = None
        date = None

        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})

        if page_section is not None:
            divs = page_section.find_all('div', recursive=False)

            if len(divs) > 1:
                target_div = divs[1]

                title_tag = target_div.find('h1')
                if title_tag:
                    title = title_tag.find('span').get_text()

                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()

                rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
                rating = rating_tag.get_text() if rating_tag else None

                plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
                plot_text = plot_tag.get_text().strip() if plot_tag else None

                if all([title, date, rating, plot_text]):
                    return [title, date, rating, plot_text]
    return None


def write_to_csv(data):
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in data:
            if row:
                movie_writer.writerow(row)


def extract_movies(soup, use_threads=True):
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    data = []

    if use_threads:
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(extract_movie_details, movie_links))
            data.extend(results)
    else:
        with Pool(processes=threads) as pool:
            results = pool.map(extract_movie_details, movie_links)
            data.extend(results)

    write_to_csv(data)


def main():
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Clear the CSV file before writing new data
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        movie_writer.writerow(['Title', 'Release Date', 'Rating', 'Plot'])

    # Extract movies using threads
    print("Extracting using threads...")
    extract_movies(soup, use_threads=True)
    end_time = time.time()
    print('Total time taken with threads: ', end_time - start_time)

    start_time = time.time()
    # Extract movies using processes
    print("Extracting using processes...")
    extract_movies(soup, use_threads=False)
    end_time = time.time()
    print('Total time taken with processes: ', end_time - start_time)


if __name__ == '__main__':
    main()
