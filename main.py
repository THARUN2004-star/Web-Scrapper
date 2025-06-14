import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import json

def comprehensive_imdb_scraper():
    """
    Comprehensive IMDb Top 250 scraper using multiple strategies
    """
    print("=== IMDb Top 250 Movies Comprehensive Scraper ===\n")
    
    # Strategy 1: Direct chart page with session handling
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    session.headers.update(headers)
    
    all_movies = []
    
    # Try multiple URLs that might contain the full list
    urls_to_try = [
        "https://www.imdb.com/chart/top/",
        "https://www.imdb.com/search/title/?groups=top_250&sort=user_rating,desc&count=250",
        "https://www.imdb.com/search/title/?groups=top_250&sort=user_rating,desc&start=1&ref_=adv_nxt",
    ]
    
    for url_index, url in enumerate(urls_to_try, 1):
        print(f"Strategy {url_index}: Trying URL: {url}")
        try:
            response = session.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract any JSON data that might contain movie information
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and 'itemListElement' in data:
                        print(f"Found JSON-LD data with {len(data['itemListElement'])} items")
                        for i, item in enumerate(data['itemListElement'], 1):
                            if 'item' in item and 'name' in item['item']:
                                movie_data = {
                                    "Rank": i,
                                    "MovieTitle": item['item']['name'],
                                    "Year": item['item'].get('datePublished', 'Unknown'),
                                    "Rating": item['item'].get('aggregateRating', {}).get('ratingValue', 'N/A'),
                                    "Votes": 'N/A'  # Not typically in JSON-LD
                                }
                                all_movies.append(movie_data)
                except json.JSONDecodeError:
                    continue
            
            if all_movies:
                print(f"Successfully extracted {len(all_movies)} movies from JSON-LD data")
                break
                
            # If JSON didn't work, try HTML parsing with multiple selectors
            selectors_to_try = [
                ('li', 'ipc-metadata-list-summary-item'),
                ('div', 'lister-item'),
                ('tr', None),  # Table rows
                ('li', 'titleColumn'),
                ('td', 'titleColumn'),
            ]
            
            for tag, class_name in selectors_to_try:
                if class_name:
                    containers = soup.find_all(tag, class_=class_name)
                else:
                    containers = soup.find_all(tag)
                
                if containers and len(containers) > 20:  # We expect a significant number
                    print(f"Found {len(containers)} containers using {tag}.{class_name if class_name else 'no-class'}")
                    
                    for i, container in enumerate(containers, 1):
                        try:
                            # Multiple strategies for title extraction
                            title = "Unknown"
                            title_selectors = [
                                ('h3', 'ipc-title__text'),
                                ('a', None),
                                ('h3', 'lister-item-header'),
                                ('.titleColumn', 'a'),
                            ]
                            
                            for t_tag, t_class in title_selectors:
                                if t_class:
                                    title_elem = container.find(t_tag, class_=t_class)
                                else:
                                    title_elem = container.find(t_tag)
                                
                                if title_elem:
                                    title_text = title_elem.get_text(strip=True)
                                    # Clean up title (remove rank numbers)
                                    title = re.sub(r'^\d+\.\s*', '', title_text)
                                    if title and title != "Unknown":
                                        break
                            
                            # Extract other data
                            year = "Unknown"
                            rating = "N/A"
                            votes = "N/A"
                            
                            # Try to extract year, rating, votes from container text
                            container_text = container.get_text()
                            
                            # Year extraction
                            year_match = re.search(r'\((\d{4})\)', container_text)
                            if year_match:
                                year = year_match.group(1)
                            
                            # Rating extraction
                            rating_match = re.search(r'(\d+\.\d+)', container_text)
                            if rating_match:
                                rating = rating_match.group(1)
                            
                            # Votes extraction
                            votes_match = re.search(r'\(([\d.]+[MK]?)\)', container_text)
                            if votes_match:
                                votes = votes_match.group(1)
                            
                            if title != "Unknown":
                                movie_data = {
                                    "Rank": i,
                                    "MovieTitle": title,
                                    "Year": year,
                                    "Rating": rating,
                                    "Votes": votes
                                }
                                all_movies.append(movie_data)
                        
                        except Exception as e:
                            continue
                    
                    if all_movies:
                        print(f"Successfully extracted {len(all_movies)} movies using HTML parsing")
                        break
            
            if all_movies:
                break
                
        except Exception as e:
            print(f"Error with strategy {url_index}: {str(e)}")
            continue
    
    return all_movies

# Run the comprehensive scraper
print("Running comprehensive IMDb scraper...")
comprehensive_movies = comprehensive_imdb_scraper()

if comprehensive_movies:
    df_comprehensive = pd.DataFrame(comprehensive_movies)
    
    # Remove duplicates and sort by rank
    df_comprehensive = df_comprehensive.drop_duplicates(subset=['MovieTitle'], keep='first')
    df_comprehensive = df_comprehensive.sort_values('Rank').reset_index(drop=True)
    
    print(f"\n=== RESULTS ===")
    print(f"Total unique movies extracted: {len(df_comprehensive)}")
    print(f"\nFirst 10 movies:")
    print(df_comprehensive.head(10).to_string(index=False))
    
    if len(df_comprehensive) >= 50:
        print(f"\nMovies 50-60:")
        print(df_comprehensive.iloc[49:59].to_string(index=False))
        
        print(f"\nLast 10 movies:")
        print(df_comprehensive.tail(10).to_string(index=False))
    
    # Save to CSV
    filename = "IMDb_Top250_Complete.csv"
    df_comprehensive.to_csv(filename, index=False)
    print(f"\nData saved to {filename}")
    
    # Display summary
    print(f"\n=== SUMMARY ===")
    print(f"Successfully scraped {len(df_comprehensive)} movies")
    print(f"Target was 250 movies")
    print(f"Success rate: {len(df_comprehensive)/250*100:.1f}%")
    
else:
    print("Failed to extract movies with comprehensive scraper")