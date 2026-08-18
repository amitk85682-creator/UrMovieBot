from flask import Blueprint, jsonify, request, send_file, render_template
from flask_cors import CORS
import os
import logging
import json
import psycopg2
from datetime import datetime
import requests
from urllib.parse import quote
import random
import re
import secrets

def register_webapp_routes(
    flask_app,
    *,
    api_movies_cache,
    search_cache,
    get_db_connection,
    close_db_connection,
    store_user_request,
    get_poster_from_tmdb_id,
    TMDB_API_KEY,
    check_secure_link,
    generate_secure_link,
    user_data_cache,
    user_data_lock,
    ADMIN_USER_ID,
    logger
):
    # 👇 NAYA FIX: UptimeRobot ke liye Root URL (Taaki 404 na aaye) 👇
    @flask_app.route('/', methods=['GET', 'HEAD'])
    def home():
        return "Bot is Alive & Running!", 200
    # 👆 NAYA FIX END 👆
    
    @flask_app.route('/api/movies', methods=['GET'])
    def get_movies():
        """
        Return list of movies with pagination (Infinite Scroll).
        """
        # Pagination Logic
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 40)) # Ek baar mein 40 movies bhejo
        
        cache_key = f"api_movies_{page}_{limit}"
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
            
        offset = (page - 1) * limit
    
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, category,
                       COALESCE(language, '') as language
                FROM movies
                WHERE poster_url IS NOT NULL AND poster_url != ''
                ORDER BY id DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            
            rows = cur.fetchall()
            movies = []
            for r in rows:
                movies.append({
                    'id': r[0],
                    'title': r[1],
                    'year': r[2] if r[2] else '',
                    'image': r[3] if r[3] else 'https://via.placeholder.com/300x450?text=No+Poster',
                    'rating': r[4] if r[4] else 'N/A',
                    'genre': r[5] if r[5] else 'Unknown',
                    'category': r[6] if r[6] else 'Movie',
                    'language': r[7]
                })
            cur.close()
            close_db_connection(conn)
            
            # Check if more movies exist
            has_more = len(movies) == limit 
            
            result = {'status': 'success', 'movies': movies, 'has_more': has_more}
            api_movies_cache.set(cache_key, result)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Error in /api/movies: {e}")
            close_db_connection(conn)
            return jsonify({'status': 'error', 'message': str(e)}), 500
    
    
    @flask_app.route('/api/movie/<int:movie_id>', methods=['GET'])
    def get_movie_details(movie_id):
        cache_key = f"api_movie_{movie_id}"
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
    
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, description, category, language, "cast", trailer_key, seasons_data
                FROM movies WHERE id = %s
            """, (movie_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
    
            movie = {
                'id': row[0],
                'title': row[1],
                'year': row[2] if row[2] else '',
                'image': row[3] if row[3] else 'https://via.placeholder.com/300x450?text=No+Poster',
                'rating': row[4] if row[4] else 'N/A',
                'genre': row[5] if row[5] else 'Unknown',
                'description': row[6] if row[6] else 'No description available.',
                'category': row[7] if row[7] else 'Movie',
                'language': row[8] if row[8] else '',
                'cast': row[9] if row[9] else '',
                'trailer_key': row[10] if row[10] else None,
                'seasons_data': row[11] if len(row) > 11 and row[11] else {}
            }
    
            # Get files
            # Updated to fetch extra_info for Season/Episode parsing
            cur.execute("SELECT quality, file_size, extra_info FROM movie_files WHERE movie_id = %s", (movie_id,))
            files = [{'quality': f[0], 'size': f[1], 'extra_info': f[2] if len(f) > 2 else ''} for f in cur.fetchall()]
            movie['files'] = files
    
            cur.close()
            close_db_connection(conn)
    
            # Trailer Fallback & Backdrop Fetch (ONLY if trailer_key is missing)
            if not movie.get('trailer_key'):
                try:
                    # Build search query with title and year if present
                    search_term = movie['title']
                    if movie['year']:
                        search_term += f" {movie['year']}"
                    search_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(search_term)}"
                    resp = requests.get(search_url, timeout=5).json()
                    if resp.get('results'):
                        first = resp['results'][0]
                        media_type = first.get('media_type', 'movie')
                        tmdb_id = first['id']
    
                        # Backdrop
                        backdrop_path = first.get('backdrop_path')
                        if backdrop_path:
                            movie['backdrop'] = f"https://image.tmdb.org/t/p/w1280{backdrop_path}"
                        else:
                            movie['backdrop'] = None
                        
                        # Fetch Trailer from TMDB
                        videos_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/videos?api_key={TMDB_API_KEY}"
                        videos = requests.get(videos_url, timeout=5).json()
                        trailer = next((v for v in videos.get('results', []) if v['type'] == 'Trailer' and v['site'] == 'YouTube'), None)
                        if trailer:
                            movie['trailer_key'] = trailer['key']
                            # 🔥 FIX: Permanently save to DB
                            try:
                                conn_update = get_db_connection()
                                if conn_update:
                                    cur_update = conn_update.cursor()
                                    cur_update.execute("UPDATE movies SET trailer_key = %s WHERE id = %s", (trailer['key'], movie_id))
                                    conn_update.commit()
                                    cur_update.close()
                                    close_db_connection(conn_update)
                            except Exception as db_e:
                                logger.error(f"Error updating trailer_key: {db_e}")
                except Exception as e:
                    logger.warning(f"TMDB fetch failed for {movie['title']}: {e}")
                    movie['backdrop'] = None
            else:
                # If we already have the trailer, skip TMDB completely and use poster as backdrop
                movie['backdrop'] = movie['image']
    
            result = {'status': 'success', 'movie': movie}
            api_movies_cache.set(cache_key, result)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Error in /api/movie/{movie_id}: {e}")
            close_db_connection(conn)
            return jsonify({'status': 'error', 'message': str(e)}), 500
    
    
    @flask_app.route('/api/search', methods=['GET'])
    def search_movies_api():
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify({'status': 'error', 'message': 'Missing query'}), 400
    
        cache_key = f"api_search_{query}"
        cached = search_cache.get(cache_key)
        if cached:
            return jsonify(cached)
    
        conn = get_db_connection()
        local_results = []
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT id, title, year, poster_url, rating, genre, category
                    FROM movies
                    WHERE title ILIKE %s OR title ILIKE %s
                    LIMIT 20
                """, (f'%{query}%', f'%{query.replace(" ", "%")}%'))
                rows = cur.fetchall()
                for r in rows:
                    local_results.append({
                        'id': r[0],
                        'title': r[1],
                        'year': r[2] if r[2] else '',
                        'image': r[3] if r[3] else 'https://via.placeholder.com/300x450?text=No+Poster',
                        'rating': r[4] if r[4] else 'N/A',
                        'genre': r[5] if r[5] else 'Unknown',
                        'category': r[6] if r[6] else 'Movie',
                        'source': 'local'
                    })
                cur.close()
            except Exception as e:
                logger.error(f"Local search error: {e}")
            finally:
                close_db_connection(conn)
    
        tmdb_results = []
        if len(local_results) < 15:
            try:
                # Use original query (including year) – no stripping
                tmdb_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(query)}"
                resp = requests.get(tmdb_url, timeout=5).json()
                for item in resp.get('results', []):
                    img_path = item.get('poster_path') or item.get('backdrop_path')
                    if not img_path:
                        continue
    
                    tmdb_results.append({
                        'id': 'tmdb_' + str(item['id']),
                        'title': item.get('title') or item.get('name') or 'Unknown',
                        'year': (item.get('release_date') or item.get('first_air_date') or '')[:4],
                        'image': f"https://image.tmdb.org/t/p/w500{img_path}",
                        'rating': round(item.get('vote_average', 0), 1),
                        'genre': 'Action, Drama',
                        'category': 'Movie' if item.get('media_type') == 'movie' else 'TV Series',
                        'source': 'tmdb',
                        'description': item.get('overview', '')
                    })
            except Exception as e:
                logger.error(f"TMDB search error: {e}")
    
        # Deduplicate: normalize title (remove punctuation, spaces, lowercase)
        def normalize_title(t):
            return re.sub(r'[^\w\s]', '', t).lower().replace(" ", "")
    
        seen = set()
        combined = []
        # Local movies first
        for m in local_results:
            key = normalize_title(m['title'])
            if key not in seen:
                seen.add(key)
                combined.append(m)
        # Then TMDB movies (only if not already seen)
        for m in tmdb_results:
            key = normalize_title(m['title'])
            if key not in seen and len(combined) < 30:
                seen.add(key)
                combined.append(m)
    
        result = {'status': 'success', 'results': combined}
        search_cache.set(cache_key, result)
        return jsonify(result)
    
    
    @flask_app.route('/api/request', methods=['POST'])
    def request_movie_api():
        """
        Store a user request from web app AND Notify Admin.
        """
        data = request.get_json()
        if not data or 'title' not in data:
            return jsonify({'status': 'error', 'message': 'Missing movie title'}), 400
        
        title = data['title'][:200]
        user_id = data.get('user_id', 0)
        username = data.get('username', '')
        first_name = data.get('first_name', 'WebApp User')
        
        success = store_user_request(user_id, username, first_name, title, None, None)
        
        if success:
            # 🔥 FIX: Web App se aayi request ko turant Admin Channel me send karein
            bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
            request_channel = os.environ.get('REQUEST_CHANNEL_ID')
            
            if bot_token and request_channel:
                try:
                    # Beautiful Admin Notification Format
                    msg_text = (
                        f"🎬 <b>New WebApp Request!</b> 🎬\n\n"
                        f"Movie: <b>{title}</b>\n"
                        f"User: {first_name} (<code>{user_id}</code>)\n"
                    )
                    if username:
                        msg_text += f"Username: @{username}\n"
                    msg_text += f"From: 🌐 Web Portal"
    
                    # Inline Buttons for Admin
                    short_title = title[:15].replace('_', ' ')
                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": "✅ Movie Add Kar Di Gai Hai", "callback_data": f"reqA_{user_id}_{short_title}"}],
                            [{"text": "❌ Nahi Mili", "callback_data": f"reqN_{user_id}_{short_title}"}]
                        ]
                    }
                    
                    # Direct Telegram API Call
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    payload = {
                        "chat_id": request_channel,
                        "text": msg_text,
                        "parse_mode": "HTML",
                        "reply_markup": reply_markup
                    }
                    requests.post(url, json=payload, timeout=5)
                except Exception as e:
                    logger.error(f"Failed to notify admin from WebApp: {e}")
    
            return jsonify({'status': 'success', 'message': 'Request saved & Admin Notified'})
        else:
            return jsonify({'status': 'error', 'message': 'Could not save request'}), 500
    
    # 🤖 GOOGLE AUTO-SUGGEST PROXY (Spelling Fixer)
    @flask_app.route('/api/suggest', methods=['GET'])
    def get_suggestions():
        q = request.args.get('q', '').strip()
        if not q:
            return jsonify([])
        try:
            # Firefox client wali API direct JSON list deti hai, jo use karne me aasan hai
            url = f"https://suggestqueries.google.com/complete/search?client=firefox&q={quote(q + ' movie')}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(url, headers=headers, timeout=3).json()
            
            # resp ka format: ["query", ["suggestion1", "suggestion2", ...]]
            suggestions = resp[1] if len(resp) > 1 else []
            
            # 'movie' word hata kar clean naam nikalna aur top 6 suggestions dikhana
            clean_suggs = [s.replace(' movie', '').title() for s in suggestions][:6] 
            return jsonify(clean_suggs)
        except Exception as e:
            logger.error(f"Suggest API Error: {e}")
            return jsonify([])
    
    @flask_app.route('/api/smart-merge', methods=['POST'])
    def smart_merge_api():
        """Hybrid Search Endpoint: Checks Local DB first (raw_query top priority), then fetches TMDB concurrently."""
        import re as re_mod
        data = request.json or {}
        queries = data.get('queries', [])
        raw_query = data.get('raw_query', '').strip()
        
        if not queries and not raw_query:
            return jsonify({'status': 'success', 'results': []})
        
        def normalize(s):
            """Strip special chars for comparison: 'Avengers: Infinity War' -> 'avengers infinity war'"""
            return re_mod.sub(r'[^a-z0-9\s]', '', s.lower()).strip()
        
        conn = get_db_connection()
        local_results = []
        found_normalized = set()
        
        # Define search queries (raw_query top priority)
        search_queries = []
        if raw_query:
            search_queries.append(raw_query)
        search_queries.extend([q for q in queries if q != raw_query])
        
        if conn:
            try:
                cur = conn.cursor()
                # Fuzzy search: Use % wildcards around each word for flexible matching
                for q in search_queries:
                    words = q.split()
                    if not words:
                        continue
                    # Build a LIKE pattern: %word1%word2%word3%
                    like_pattern = '%' + '%'.join(words) + '%'
                    cur.execute("""
                        SELECT id, title, year, poster_url, rating, genre, category 
                        FROM movies 
                        WHERE title ILIKE %s
                        LIMIT 1
                    """, (like_pattern,))
                    row = cur.fetchone()
                    if row:
                        title = row[1]
                        norm_title = normalize(title)
                        if norm_title not in found_normalized:
                            found_normalized.add(norm_title)
                            local_results.append({
                                'id': row[0],
                                'title': title,
                                'year': row[2] if row[2] else '',
                                'image': row[3] if row[3] else 'https://via.placeholder.com/300x450?text=No+Poster',
                                'rating': row[4] if row[4] else 'N/A',
                                'genre': row[5] if row[5] else 'Unknown',
                                'category': row[6] if row[6] else 'Movie',
                                'source': 'local',
                                '_query': q  # Track which query matched
                            })
                cur.close()
            except Exception as e:
                logger.error(f"Error in smart_merge local check: {e}")
            finally:
                close_db_connection(conn)
    
        # Find which queries still need TMDB lookup
        matched_queries = {r['_query'] for r in local_results}
        missing_queries = [q for q in search_queries if q not in matched_queries]
        tmdb_results = []
        
        def fetch_tmdb(q):
            try:
                url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(q)}"
                resp = requests.get(url, timeout=3).json()
                results = resp.get('results', [])
                if results:
                    item = results[0]
                    img_path = item.get('poster_path') or item.get('backdrop_path')
                    if img_path:
                        return {
                            'id': 'tmdb_' + str(item['id']),
                            'title': item.get('title') or item.get('name') or 'Unknown',
                            'year': (item.get('release_date') or item.get('first_air_date') or '')[:4],
                            'image': f"https://image.tmdb.org/t/p/w500{img_path}",
                            'rating': round(item.get('vote_average', 0), 1),
                            'genre': 'Action, Drama',
                            'category': 'Movie' if item.get('media_type') == 'movie' else 'TV Series',
                            'source': 'tmdb',
                            'description': item.get('overview', ''),
                            '_query': q
                        }
            except Exception as e:
                logger.error(f"TMDB fetch error for {q}: {e}")
            return None
    
        # Fetch concurrently for maximum speed
        if missing_queries:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_tmdb, q): q for q in missing_queries}
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res:
                        tmdb_results.append(res)
        
        # Build final results ordered by original query order (raw_query first)
        all_fetched = local_results + tmdb_results
        final_results = []
        seen_ids = set()
        
        for q in search_queries:
            # Find the result that was fetched for this query
            match = next((r for r in all_fetched if r.get('_query') == q), None)
            if match and match['id'] not in seen_ids:
                seen_ids.add(match['id'])
                # Remove internal tracking key before sending to frontend
                result_copy = {k: v for k, v in match.items() if k != '_query'}
                final_results.append(result_copy)
                    
        return jsonify({'status': 'success', 'results': final_results})
    
    @flask_app.route('/api/imdb_id/<int:tmdb_id>', methods=['GET'])
    def get_imdb_id(tmdb_id):
        """Fetch IMDB ID from TMDB API for the Web Player streamimdb.ru requirement."""
        try:
            url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
            resp = requests.get(url, timeout=5).json()
            imdb_id = resp.get('imdb_id')
            
            if imdb_id:
                return jsonify({'status': 'success', 'imdb_id': imdb_id})
            
            # Fallback to TV show if movie returns no IMDB ID
            url_tv = f"https://api.themoviedb.org/3/tv/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
            resp_tv = requests.get(url_tv, timeout=5).json()
            imdb_id_tv = resp_tv.get('imdb_id')
            
            if imdb_id_tv:
                 return jsonify({'status': 'success', 'imdb_id': imdb_id_tv})
                 
            return jsonify({'status': 'error', 'message': 'No IMDB ID found'}), 404
        except Exception as e:
            logger.error(f"Error fetching IMDB ID for tmdb_{tmdb_id}: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500
    
    # ==================== MAIN WEB APP PAGE (Premium HTML) ====================
    
    # 🛡️ MIDDLEMAN REDIRECT PAGE (Anti-Bot)
    @flask_app.route('/watch/<int:movie_id>')
    def secure_watch(movie_id):
        # Yeh HTML page user ko dikhega. Bots JS run nahi kar pate.
        html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>FlimfyBox - Verifying Secure Connection...</title>
            <style>
                body { background: #09090b; color: white; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; font-family: sans-serif; }
                .loader { border: 4px solid rgba(255,255,255,0.1); border-top: 4px solid #f43f5e; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin-bottom: 20px; }
                @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
            </style>
        </head>
        <body>
            <div class="loader"></div>
            <h3>Securely verifying your connection...</h3>
            <p style="color: #a1a1aa; font-size: 13px;">Please wait 2 seconds. You will be redirected automatically.</p>
            
            <script>
                // Invisible JS Challenge
                setTimeout(() => {
                    fetch('/api/gen_link/""" + str(movie_id) + """', { method: 'POST' })
                    .then(response => response.json())
                    .then(data => {
                        if(data.url) {
                            window.location.href = data.url; 
                        } else {
                            document.body.innerHTML = "<h3>❌ Server Error. Please try again.</h3>";
                        }
                    }).catch(e => {
                        document.body.innerHTML = "<h3>❌ Connection failed.</h3>";
                    });
                }, 1500); 
            </script>
        </body>
        </html>
        """
        return html
    
    # 🔐 SECRET LINK GENERATOR API (Auto Delete Logic)
    @flask_app.route('/api/gen_link/<int:movie_id>', methods=['POST'])
    def gen_secure_link(movie_id):
        token = "tmp_" + secrets.token_hex(6)
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                # Delete old tokens (1 minute se purane)
                cur.execute("DELETE FROM temp_links WHERE created_at < NOW() - INTERVAL '1 minute'")
                # Save new token
                cur.execute("INSERT INTO temp_links (token, movie_id) VALUES (%s, %s)", (token, movie_id))
                conn.commit()
                cur.close()
            except Exception as e:
                logger.error(f"Token Error: {e}")
            finally:
                close_db_connection(conn)
                
        bot_username = os.environ.get('BOT_USERNAME', 'FlimfyBoxBot')
        tg_url = f"tg://resolve?domain={bot_username}&start={token}"
        return jsonify({"url": tg_url})
    
    @flask_app.route('/webapp')
    def serve_mini_app():
        return render_template("mini_app.html")
    
    # ==================== RUN FLASK ====================
    
