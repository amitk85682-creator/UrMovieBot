const tg = window.Telegram.WebApp;
        tg.expand();
        tg.ready();
        const BOT_USERNAME = "FlimfyBoxBot"; // change if needed

        // State
        let allMovies = [];
        let tmdbMoviesMap = {};

        // Utility
        function showToast(msg) {
            const t = document.getElementById('toast');
            t.innerText = msg;
            t.classList.add('show');
            setTimeout(() => t.classList.remove('show'), 2500);
        }

        function scrollRow(elementId, amount) {
            const el = document.getElementById(elementId);
            if (el) el.scrollBy({ left: amount, behavior: 'smooth' });
        }

        // Pagination State
        let currentPage = 1;
        let isFetching = false;
        let hasMoreMovies = true;

        // Load movies from API with Infinite Scroll support
        async function loadMovies(page = 1) {
            if (isFetching || !hasMoreMovies) return;
            isFetching = true;

            try {
                // Agar page 1 se zyada hai, toh neeche ek loading spinner dikhao
                if (page > 1) {
                    document.getElementById('moreGrid').insertAdjacentHTML('beforeend', '<div id="scrollLoader" style="grid-column: 1 / -1; text-align: center; padding: 20px;"><div class="loader" style="width:30px;height:30px;border-width:3px;margin:0 auto;"></div></div>');
                }

                const res = await fetch(`/api/movies?page=${page}&limit=40`);
                const data = await res.json();
                
                // Naya data aate hi loader hata do
                if (page > 1) {
                    const loader = document.getElementById('scrollLoader');
                    if (loader) loader.remove();
                }

                if (data.status === 'success') {
                    const newMovies = data.movies.filter(m => m.image);
                    hasMoreMovies = data.has_more; 
                    
                    if (page === 1) {
                        // Pehli baar: Pura UI setup karo
                        allMovies = newMovies;
                        renderHome(allMovies); 
                        renderGenrePills(allMovies);
                    } else {
                        // Scrolling par: Purani movies mein nayi jod do
                        allMovies = [...allMovies, ...newMovies]; 
                        const newCardsHTML = renderCards(newMovies, 'grid-card', false);
                        document.getElementById('moreGrid').insertAdjacentHTML('beforeend', newCardsHTML);
                    }
                    currentPage++; // Agli baar ke liye page badha do
                } else {
                    console.error('API error:', data.message);
                }
            } catch (e) {
                console.error('Fetch failed', e);
            } finally {
                isFetching = false;
            }
        }

        // 🔥 NAYA: Infinite Scroll Listener
        window.addEventListener('scroll', () => {
            // Agar user page ke bottom se 600px upar hai, toh advance mein next page load kar lo
            if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 600) {
                // Check karo ki normal page open hai (Search result open na ho)
                if (document.getElementById('searchResultsContent').style.display === 'none') {
                    loadMovies(currentPage);
                }
            }
        });

        function renderGenrePills(movies) {
            const genreSet = new Set();
            movies.forEach(m => {
                if (m.genre && m.genre !== 'Unknown') {
                    m.genre.split(',').forEach(g => genreSet.add(g.trim()));
                }
            });
            const genres = ['All', ...Array.from(genreSet).slice(0, 10)];
            const container = document.getElementById('genreContainer');
            container.innerHTML = genres.map(g => `<div class="genre-pill ${g==='All'?'active':''}" onclick="filterByGenre('${g}', this)">${g}</div>`).join('');
        }

        window.filterByGenre = function(genre, el) {
            document.querySelectorAll('.genre-pill').forEach(p => p.classList.remove('active'));
            el.classList.add('active');
            if (genre === 'All') {
                renderHome(allMovies);
            } else {
                const filtered = allMovies.filter(m => m.genre && m.genre.includes(genre));
                // hide rows and show only grid with filtered
                document.querySelectorAll('.movie-row').forEach(r => r.style.display = 'none');
                document.getElementById('moreGrid').innerHTML = renderCards(filtered, 'grid-card', false);
            }
        };

        function renderHome(movies) {
            // Hero slider
            if (movies.length > 0) {
                let idx = 0;
                const top5 = movies.slice(0, 5);
                const updateHero = () => {
                    const m = top5[idx];
                    document.getElementById('heroSlider').style.backgroundImage = `url(${m.image})`;
                    document.getElementById('heroTitle').innerText = m.title;
                    document.getElementById('heroMeta').innerText = `${m.year} • ${m.category}`;
                    idx = (idx + 1) % top5.length;
                };
                updateHero();
                setInterval(updateHero, 5000);
            }

            // Trending (first 15)
            document.getElementById('trendingScroll').innerHTML = renderCards(movies.slice(0, 15), 'card', false);
            // Bollywood
            const bolly = movies.filter(m => m.category?.toLowerCase().includes('bollywood')).slice(0, 15);
            document.getElementById('bollywoodScroll').innerHTML = renderCards(bolly, 'card', false);
            // Hollywood
            const holy = movies.filter(m => m.category?.toLowerCase().includes('hollywood')).slice(0, 15);
            document.getElementById('hollywoodScroll').innerHTML = renderCards(holy, 'card', false);
            // Show remaining movies from the first batch
            document.getElementById('moreGrid').innerHTML = renderCards(movies.slice(15), 'grid-card', false);
        }

        function renderCards(movies, cardClass = 'card', forceTMDB = false) {
            if (!movies.length) return '<div style="color:var(--text-muted); padding:10px;">No movies</div>';
            return movies.map(m => {
                // 🔥 FIX: Automatically detect karega ki poster TMDB (Request) ka hai ya Local DB ka
                const isTMDB = forceTMDB || m.source === 'tmdb'; 
                const rating = m.rating && m.rating !== 'N/A' ? `⭐ ${m.rating}` : '';
                const badge = isTMDB ? '<div class="card-rating" style="color:white; background:var(--primary);">Request</div>' : (rating ? `<div class="card-rating">${rating}</div>` : '');
                
                return `
                    <div class="${cardClass}" onclick="openDetails('${m.id}', ${isTMDB})">
                        <img src="${m.image}" class="card-img" loading="lazy" onerror="this.src='https://via.placeholder.com/300x450?text=No+Poster'">
                        ${badge}
                        <div class="card-title">${m.title}</div>
                        <div class="card-meta">${m.year || ''}</div>
                    </div>
                `;
            }).join('');
        }

// Hybrid Search
let searchTimeout;
let searchRequestId = 0;

document.getElementById('searchInput').addEventListener('input', (e) => {
    clearTimeout(searchTimeout);
    const q = e.target.value.trim();
    const dropdown = document.getElementById('searchDropdown');
    
    if (!q) {
        dropdown.classList.remove('active');
        return;
    }
    
    dropdown.innerHTML = '<div class="loader">Loading suggestions...</div>';
    dropdown.classList.add('active');

    searchTimeout = setTimeout(() => {
        searchRequestId++;
        const currentId = searchRequestId;
        
        // Remove old script FIRST to prevent pile-up
        const oldScript = document.getElementById('googleSuggestScript');
        if (oldScript) oldScript.remove();
        
        // Set callback BEFORE injecting script
        window.googleSuggestCb = async function(data) {
            if (currentId !== searchRequestId) return;
            const s = document.getElementById('googleSuggestScript');
            if (s) s.remove();
            
            let suggs = data[1] || [];
            suggs = suggs.map(x => x.replace(/ movie$/i, '').trim()
                                     .replace(/\b\w/g, c => c.toUpperCase())).slice(0, 5);
            
            if(suggs.length > 0) {
                let html = suggs.map((title, i) => `
                    <div class="search-item skeleton-item" id="skel-${i}">
                        <div class="skeleton-poster"></div>
                        <div class="search-item-info">
                            <div class="search-item-title">${title}</div>
                            <div class="search-item-meta" style="color:var(--text-muted);font-size:11px;">Loading...</div>
                        </div>
                    </div>
                `).join('');
                dropdown.innerHTML = html;
                
                try {
                    const mergeRes = await fetch('/api/smart-merge', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ queries: suggs, raw_query: q })
                    });
                    const mergeData = await mergeRes.json();
                    if (currentId !== searchRequestId) return;
                    
                    if (mergeData.status === 'success' && mergeData.results.length > 0) {
                        const results = mergeData.results;
                        results.forEach(r => {
                            if (r.source === 'tmdb') { tmdbMoviesMap[r.id] = r; }
                            else if (!allMovies.find(m => m.id == r.id)) { allMovies.push(r); }
                        });

                        let newHtml = '';
                        results.forEach(r => {
                            const isTMDB = r.source === 'tmdb';
                            const rating = r.rating && r.rating !== 'N/A' ? '<span>\u2b50 ' + r.rating + '</span>' : '';
                            const year = r.year ? '<span>' + r.year + '</span>' : '';
                            if (isTMDB) {
                                const rawTmdbId = r.id.replace('tmdb_', '');
                                newHtml += '<div class="search-item fade-in">'
                                    + '<img src="' + r.image + '" loading="lazy" onerror="this.src=\'https://via.placeholder.com/50x75?text=No+Poster\'">'
                                    + '<div class="search-item-info">'
                                    + '<div class="search-item-title">' + r.title + '</div>'
                                    + '<div class="search-item-meta">' + year + ' ' + rating + '</div></div>'
                                    + '<div class="search-actions">'
                                    + '<button class="btn-sm btn-sm-outline" onclick="requestMovie(\'' + r.title.replace(/'/g, "\\'") + '\')">Request Now</button>'
                                    + '<button class="btn-sm btn-sm-primary" onclick="openWebPlayer(\'' + rawTmdbId + '\')">Watch Online</button>'
                                    + '</div></div>';
                            } else {
                                newHtml += '<div class="search-item fade-in" onclick="openDetails(\'' + r.id + '\', false); document.getElementById(\'searchDropdown\').classList.remove(\'active\');">'
                                    + '<img src="' + r.image + '" loading="lazy" onerror="this.src=\'https://via.placeholder.com/50x75?text=No+Poster\'">'
                                    + '<div class="search-item-info">'
                                    + '<div class="search-item-title">' + r.title + '</div>'
                                    + '<div class="search-item-meta">' + year + ' ' + rating + ' <span style="color:var(--primary); font-weight:bold;">Available</span></div></div>'
                                    + '<div class="search-actions"><button class="btn-sm btn-sm-primary">View Now</button></div></div>';
                            }
                        });
                        dropdown.innerHTML = newHtml;
                    } else {
                        const currentQ = document.getElementById('searchInput').value.trim();
                        dropdown.innerHTML = '<div style="text-align:center; padding: 20px;" class="fade-in">'
                            + '<p style="color: var(--text-muted); margin-bottom: 15px;">We could not find "' + currentQ + '".</p>'
                            + '<button onclick="requestSilent(\'' + currentQ.replace(/'/g, "\\'") + '\')" class="request-glow-btn">'
                            + '<i class="fas fa-paper-plane"></i> Request This Movie</button></div>';
                    }
                } catch (e) {
                    console.error(e);
                    dropdown.innerHTML = '<div class="loader">Error loading details</div>';
                }
            } else {
                const currentQ = document.getElementById('searchInput').value.trim();
                dropdown.innerHTML = '<div style="text-align:center; padding: 20px;">'
                    + '<p style="color: var(--text-muted); margin-bottom: 15px;">We could not find "' + currentQ + '".</p>'
                    + '<button onclick="requestSilent(\'' + currentQ.replace(/'/g, "\\'") + '\')" class="request-glow-btn">'
                    + '<i class="fas fa-paper-plane"></i> Request This Movie</button></div>';
            }
        };
        
        const script = document.createElement('script');
        script.id = 'googleSuggestScript';
        script.src = 'https://suggestqueries.google.com/complete/search?client=chrome&q=' + encodeURIComponent(q + ' movie') + '&callback=googleSuggestCb';
        script.onerror = function() {
            dropdown.innerHTML = '<div class="loader">Network Error</div>';
            this.remove();
        };
        document.head.appendChild(script);
    }, 300);
});


// Hide dropdown if clicked outside
document.addEventListener('click', (e) => {
    const dropdown = document.getElementById('searchDropdown');
    const container = document.querySelector('.search-section');
    if (!container.contains(e.target)) {
        dropdown.classList.remove('active');
    }
});
        // Details
        window.openDetails = function(id, isTMDB) {
            const movie = isTMDB ? tmdbMoviesMap[id] : allMovies.find(m => m.id == id);
            if (!movie) return;
            if (isTMDB) {
                // show request button
                const backdropImg = movie.image; // fallback
                document.getElementById('dpBackdrop').style.backgroundImage = `url(${backdropImg})`;
                document.getElementById('dpFloatPoster').src = movie.image;
                document.getElementById('dpTitle').innerText = movie.title;
                document.getElementById('dpRating').innerText = movie.rating || 'N/A';
                document.getElementById('dpGenre').innerText = movie.genre || 'Action, Drama';
                document.getElementById('dpDesc').innerText = movie.description || 'No description available.';
                document.getElementById('castSection').innerHTML = '';
                document.getElementById('dpTrailerBtn').innerHTML = `<button class="btn-request" onclick="requestMovie('${movie.title}')"><i class="fas fa-hand-paper"></i> Request This Title</button>`;
                document.getElementById('dpLinks').innerHTML = '';
                document.getElementById('detailsPage').classList.add('open');
                return;
            }

            // Fetch full details
            fetch(`/api/movie/${id}`)
                .then(res => res.json())
                .then(data => {
                    if (data.status === 'success') {
                        const m = data.movie;
                        // Set backdrop (use TMDB backdrop if exists, else poster)
                        const backdropUrl = m.backdrop ? m.backdrop : m.image;
                        document.getElementById('dpBackdrop').style.backgroundImage = `url(${backdropUrl})`;
                        document.getElementById('dpFloatPoster').src = m.image;
                        document.getElementById('dpTitle').innerText = m.title;
                        document.getElementById('dpRating').innerText = m.rating + '/10';
                        document.getElementById('dpGenre').innerText = m.genre;
                        document.getElementById('dpDesc').innerText = m.description;
                        // Cast
                        // Cast Chips (Local Fetch)
                        if (m.cast && m.cast.trim().length > 0) {
                            const actors = m.cast.split(',');
                            let castHtml = '<div style="margin-bottom:20px;">';
                            actors.forEach(actor => {
                                const cleanName = actor.trim();
                                if (cleanName) {
                                    castHtml += `<span class="cast-chip">[ ${cleanName} ]</span>`;
                                }
                            });
                            castHtml += '</div>';
                            document.getElementById('castSection').innerHTML = castHtml;
                        } else {
                            document.getElementById('castSection').innerHTML = '';
                        }
                        // Trailer button (Strict In-App Play Only)
                        if (m.trailer_key) {
                            document.getElementById('dpTrailerBtn').innerHTML = `<button class="btn-trailer" onclick="playTrailer('${m.trailer_key}')"><i class="fab fa-youtube"></i> Watch Trailer</button>`;
                        } else {
                            document.getElementById('dpTrailerBtn').innerHTML = `<button class="btn-trailer" onclick="showToast('❌ Trailer not found')"><i class="fas fa-video-slash"></i> No Trailer</button>`;
                        }
                        // Download links
                        if (m.files && m.files.length) {
                            let links = '<div class="dl-heading">AVAILABLE QUALITIES</div>';
                            m.files.forEach(f => {
                                links += `
                                    <button class="dl-btn" onclick="downloadMovie(${m.id})">
                                        <span class="quality-text">📁 ${f.quality} <span class="file-size">[${f.size || 'N/A'}]</span></span>
                                        <span class="action">Get</span>
                                    </button>
                                `;
                            });
                            document.getElementById('dpLinks').innerHTML = links;
                        } else {
                            document.getElementById('dpLinks').innerHTML = `
                                <div class="dl-heading">DOWNLOAD</div>
                                <button class="dl-btn" onclick="downloadMovie(${m.id})">
                                    <span class="quality-text">📁 1080p Full HD</span>
                                    <span class="action">Get</span>
                                </button>
                            `;
                        }
                        document.getElementById('detailsPage').classList.add('open');
                    }
                });
        };

        window.closeDetails = function() {
            document.getElementById('detailsPage').classList.remove('open');
        };

        window.playTrailer = function(key) {
            document.getElementById('trailerIframe').src = `https://www.youtube.com/embed/${key}?autoplay=1&rel=0`;
            document.getElementById('trailerModal').classList.add('active');
        };

        window.closeTrailer = function() {
            document.getElementById('trailerIframe').src = '';
            document.getElementById('trailerModal').classList.remove('active');
        };

        // In-App Web Player Modal Logic
        window.openWebPlayer = function(tmdbId) {
            document.getElementById('searchDropdown').classList.remove('active');
            const modal = document.getElementById('webPlayerModal');
            const iframeCont = document.getElementById('wpIframeContainer');
            const titleEl = document.getElementById('wpTitle');
            
            modal.classList.add('active');
            titleEl.innerText = 'Loading Player...';
            iframeCont.innerHTML = '<div class="wp-loader"><div class="loader"></div>Fetching Secure Stream...</div>';
            
            // Background async call to fetch IMDb ID
            fetch(`/api/imdb_id/${tmdbId}`)
                .then(res => res.json())
                .then(data => {
                    if (data.status === 'success' && data.imdb_id) {
                        titleEl.innerText = 'Secure Player · Premium Stream';
                        // streamimdb.ru requires IMDb ID for playing
                        iframeCont.innerHTML = `<iframe src="https://streamimdb.ru/embed/movie/${data.imdb_id}" allowfullscreen allow="autoplay"></iframe>`;
                    } else {
                        titleEl.innerText = 'Error loading stream';
                        iframeCont.innerHTML = '<div style="color:white;text-align:center;">❌ Could not find streaming source. Try Requesting the movie instead.</div>';
                    }
                })
                .catch(e => {
                    titleEl.innerText = 'Network Error';
                    iframeCont.innerHTML = '<div style="color:white;text-align:center;">❌ Network error while loading player.</div>';
                });
        };

        window.closeWebPlayer = function() {
            document.getElementById('webPlayerModal').classList.remove('active');
            document.getElementById('wpIframeContainer').innerHTML = ''; // Stop video playback
        };

        window.requestMovie = function(title) {
            tg.HapticFeedback.notificationOccurred('success');
            showToast('⏳ Requesting...');
            const user = tg.initDataUnsafe?.user || {id: 0, username: 'webapp', first_name: 'User'};
            fetch('/api/request', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title, user_id: user.id, username: user.username, first_name: user.first_name})
            })
            .then(r => r.json())
            .then(d => {
                if (d.status === 'success') showToast('✅ Request sent!');
                else showToast('❌ Failed');
            })
            .catch(() => showToast('❌ Error'));
        };

        // 🔥 NAYA: Silent Request (Jab TMDB aur Google dono fail ho jayein)
        window.requestSilent = function(title) {
            tg.HapticFeedback.notificationOccurred('success');
            showToast('⏳ Sending Request...');
            const user = tg.initDataUnsafe?.user || {id: 0, username: 'webapp', first_name: 'User'};
            
            fetch('/api/request', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title: title, user_id: user.id, username: user.username, first_name: user.first_name})
            })
            .then(r => r.json())
            .then(d => {
                if (d.status === 'success') {
                    showToast('✅ Request Sent to Admin!');
                    // Request bhejte hi Mini app close kar do (Seamless feel ke liye)
                    setTimeout(() => { tg.close(); }, 1500);
                } else {
                    showToast('❌ Failed to send');
                }
            })
            .catch(() => showToast('❌ Network Error'));
        };

        // 🛡️ NAYA: Anti-Bot Middleware Par Bhejne Wala Function
        window.downloadBot = function(id) {
            tg.HapticFeedback.impactOccurred('heavy');
            // Seedha Bot ki jagah pehle Secure verification page par bhejenge
            tg.openLink(`https://flimfybox-bot-yht0.onrender.com/watch/${id}`);
        };

        window.downloadMovie = function(id) {
            tg.HapticFeedback.impactOccurred('heavy');
            tg.openLink(`https://flimfybox-bot-yht0.onrender.com/watch/${id}`);
        };

        // Start
        loadMovies();
        
        // 🪄 NAYA JUGAD: URL se query nikal kar auto-search karna
        setTimeout(() => {
            const urlParams = new URLSearchParams(window.location.search);
            const reqQuery = urlParams.get('req');
            
            if (reqQuery) {
                const searchInput = document.getElementById('searchInput');
                searchInput.value = reqQuery;
                showToast("🔍 Finding correct spelling...");
                // Search ko trigger karo
                searchInput.dispatchEvent(new Event('input', { bubbles: true }));
            }
        }, 500); // Thoda ruk kar karenge taaki app load ho jaye