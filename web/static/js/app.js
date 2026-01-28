const API_BASE = '';
        let currentPage = 1;
        let totalPages = 1;
        let currentLimit = 50;
        let allSpeakers = [];
        let selectedSpeaker = '';
        let audioCacheBust = {}; // {fileId: timestamp} - для антикэша после trim

        // ============================================================
        // STATS
        // ============================================================
        async function loadStats() {
            try {
                const res = await fetch(`${API_BASE}/api/stats`);
                const data = await res.json();
                if (data.success) {
                    document.getElementById('stat-total').textContent = data.data.total || 0;
                    document.getElementById('stat-verified').textContent = data.data.verified || 0;
                    document.getElementById('stat-needs-review').textContent = data.data.needs_review || 0;
                    document.getElementById('stat-pending').textContent = data.data.pending || 0;
                    document.getElementById('stat-pending-nolm').textContent = data.data.pending_nolm || 0;
                    document.getElementById('stat-pending-whisper-local').textContent = data.data.pending_whisper_local || 0;
                    document.getElementById('stat-pending-whisper-openai').textContent = data.data.pending_whisper_openai || 0;

                    // WER
                    document.getElementById('stat-kaldi-wer').textContent = data.data.kaldi_wer ? (data.data.kaldi_wer * 100).toFixed(2) + '%' : '-';
                    document.getElementById('stat-kaldi-nolm-wer').textContent = data.data.kaldi_nolm_wer ? (data.data.kaldi_nolm_wer * 100).toFixed(2) + '%' : '-';
                    document.getElementById('stat-whisper-local-wer').textContent = data.data.whisper_local_wer ? (data.data.whisper_local_wer * 100).toFixed(2) + '%' : '-';
                    document.getElementById('stat-whisper-openai-wer').textContent = data.data.whisper_openai_wer ? (data.data.whisper_openai_wer * 100).toFixed(2) + '%' : '-';

                    // Processed counts
                    document.getElementById('stat-kaldi-processed').textContent = data.data.processed || 0;
                    document.getElementById('stat-nolm-processed').textContent = data.data.processed_nolm || 0;
                    document.getElementById('stat-wlocal-processed').textContent = data.data.processed_whisper_local || 0;
                    document.getElementById('stat-wopenai-processed').textContent = data.data.processed_whisper_openai || 0;
                }
            } catch (e) {
                console.error('Failed to load stats:', e);
            }
        }

        // ============================================================
        // PROCESSING - добавлен kaldi-nolm
        // ============================================================

        async function startProcessing() {
            const limit = document.getElementById('process-limit').value;
            const target = document.getElementById('process-target').value;

            let url = '';
            switch (target) {
                case 'kaldi':
                    url = `${API_BASE}/api/asr/start?limit=${limit}&workers=5`;
                    break;
                case 'kaldi-nolm':
                    url = `${API_BASE}/api/asr-nolm/start?limit=${limit}&workers=5`;
                    break;
                case 'whisper-local':
                    url = `${API_BASE}/api/whisper-local/start?limit=${limit}&workers=3`;
                    break;
                case 'whisper-openai':
                    url = `${API_BASE}/api/whisper-openai/start?limit=${limit}&workers=3&min_wer=0`;
                    break;
                case 'whisper-openai-forced':
                    url = `${API_BASE}/api/whisper-openai/start-forced?limit=${limit}&workers=3`;
                    break;
                case 'analyze':
                    const force = document.getElementById('analyze-force')?.checked ? '1' : '0';
                    url = `${API_BASE}/api/analyze/start?limit=${limit}&force=${force}`;
                    break;
            }

            try {
                const res = await fetch(url, { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    showProcessStatus(`Started ${target}: ${data.data.message || 'OK'}`);
                    setTimeout(refreshStatus, 1000);
                } else {
                    showProcessStatus(`Error: ${data.error}`, true);
                }
            } catch (e) {
                showProcessStatus(`Error: ${e.message}`, true);
            }
        }

        async function stopProcessing() {
            const target = document.getElementById('process-target').value;

            let url = '';
            switch (target) {
                case 'kaldi':
                    url = `${API_BASE}/api/asr/stop`;
                    break;
                case 'kaldi-nolm':
                    url = `${API_BASE}/api/asr-nolm/stop`;
                    break;
                case 'whisper-local':
                    url = `${API_BASE}/api/whisper-local/stop`;
                    break;
                case 'whisper-openai':
                case 'whisper-openai-forced':
                    url = `${API_BASE}/api/whisper-openai/stop`;
                    break;
            }

            try {
                const res = await fetch(url, { method: 'POST' });
                const data = await res.json();
                showProcessStatus(`Stopped ${target}`);
            } catch (e) {
                showProcessStatus(`Error: ${e.message}`, true);
            }
        }

        async function refreshStatus() {
            const target = document.getElementById('process-target').value;

            let url = '';
            switch (target) {
                case 'kaldi':
                    url = `${API_BASE}/api/asr/status`;
                    break;
                case 'kaldi-nolm':
                    url = `${API_BASE}/api/asr-nolm/status`;
                    break;
                case 'whisper-local':
                    url = `${API_BASE}/api/whisper-local/status`;
                    break;
                case 'whisper-openai':
                case 'whisper-openai-forced':
                    url = `${API_BASE}/api/whisper-openai/status`;
                    break;
                case 'analyze':
                    url = `${API_BASE}/api/analyze/status`;
                    break;
            }

            if (!url) {
                showProcessStatus('No status endpoint for this target');
                return;
            }

            try {
                const res = await fetch(url);
                const data = await res.json();
                if (data.success && data.data) {
                    const s = data.data;
                    if (s.running) {
                        showProcessStatus(`Running: ${s.processed}/${s.total} (${s.percent?.toFixed(1)}%)`);
                        setTimeout(refreshStatus, 2000);
                    } else {
                        showProcessStatus(`Idle. Last: ${s.processed || 0} processed`);
                        loadFiles();
                        loadStats();
                    }
                }
            } catch (e) {
                showProcessStatus(`Error: ${e.message}`, true);
            }
        }


        function showProcessStatus(msg, isError = false) {
            const el = document.getElementById('process-status');
            el.textContent = msg;
            el.className = isError ? 'text-xs text-red-600 ml-2' : 'text-xs text-gray-600 ml-2';
        }

        // ============================================================
        // PROCESS SINGLE FILE - добавлен kaldi-nolm
        // ============================================================

        async function processFile(fileId, target) {
            try {
                const res = await fetch(`${API_BASE}/api/process/${fileId}?target=${target}`, { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    showProcessStatus(`Processing ${target} for file #${fileId}`);
                    setTimeout(() => loadFiles(), 2000);
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        // ============================================================
        // VERIFICATION - новые функции
        // ============================================================

        async function verifyFile(fileId) {
            try {
                const res = await fetch(`${API_BASE}/api/files/${fileId}/verify`, { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    loadFiles();
                    loadStats();
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        async function unverifyFile(fileId) {
            try {
                const res = await fetch(`${API_BASE}/api/files/${fileId}/unverify`, { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    loadFiles();
                    loadStats();
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        async function deleteFile(fileId) {
            if (!confirm(`Delete file #${fileId}?\n\nThis will permanently delete the audio file and database record!`)) {
                return;
            }
            
            try {
                const res = await fetch(`${API_BASE}/api/files/${fileId}`, { method: 'DELETE' });
                const data = await res.json();
                if (data.success) {
                    showToast(`Deleted file #${fileId}`);
                    loadFiles();
                    loadStats();
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        // ============================================================
        // EDIT TRANSCRIPTION - новая функция
        // ============================================================

        async function saveTranscription(fileId) {
            const textarea = document.getElementById(`edit-orig-${fileId}`);
            const text = textarea.value;

            try {
                const res = await fetch(`${API_BASE}/api/files/${fileId}/transcription`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ transcription: text })
                });
                const data = await res.json();
                if (data.success) {
                    alert('Сохранено! WER пересчитан для всех ASR.');
                    closeModal();
                    loadFiles();
                    loadStats();
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        // ============================================================
        // FILTERS
        // ============================================================

        function resetFilters() {
            document.getElementById('filter').value = 'all';
            document.getElementById('filter-verified').value = '';
            document.getElementById('filter-id').value = '';
            document.getElementById('wer-op').value = '';
            document.getElementById('wer-value').value = '';
            document.getElementById('wer-nolm-op').value = '';
            document.getElementById('wer-nolm-value').value = '';
            document.getElementById('wer-wl-op').value = '';
            document.getElementById('wer-wl-value').value = '';
            document.getElementById('snr-op').value = '';
            document.getElementById('snr-value').value = '';
            document.getElementById('dur-op').value = '';
            document.getElementById('dur-value').value = '';
            document.getElementById('filter-text').value = '';
            document.getElementById('filter-chapter').value = '';
            document.getElementById('filter-noise').value = '';
            clearSpeaker();
            currentPage = 1;
            loadFiles();
        }

        // ============================================================
        // LOAD FILES - обновлён с verified фильтром
        // ============================================================

        async function loadFiles() {
            const filter = document.getElementById('filter').value;
            const filterVerified = document.getElementById('filter-verified').value;
            currentLimit = parseInt(document.getElementById('limit').value);

            const werOp = document.getElementById('wer-op').value;
            const werValue = document.getElementById('wer-value').value;
            const durOp = document.getElementById('dur-op').value;
            const durValue = document.getElementById('dur-value').value;

            const fileList = document.getElementById('file-list');
            fileList.classList.add('loading');

            try {
                let url = `${API_BASE}/api/files?page=${currentPage}&limit=${currentLimit}`;

                // Verified filter
                if (filterVerified) {
                    url += `&verified=${filterVerified}`;
                }

                // Merged/Dataset filter — ПЕРЕНЕСЕНО СЮДА
                const filterMerged = document.getElementById('filter-merged').value;
                if (filterMerged) {
                    url += `&merged=${filterMerged}`;
                }

                // Active filter
                const filterActive = document.getElementById('filter-active')?.value || 'yes';
                if (filterActive === 'all') {
                    url += '&active=all';
                } else if (filterActive === 'no') {
                    url += '&active=no';
                }

                // Preset filters
                if (filter === 'errors') url += '&wer_op=gt&wer_value=0';
                if (filter === 'high_wer') url += '&wer_op=gt&wer_value=10';
                if (filter === 'very_high_wer') url += '&wer_op=gt&wer_value=20';
                if (filter === 'pending_asr') url += '&asr_status=pending';
                if (filter === 'pending_nolm') url += '&asr_nolm_status=pending';
                if (filter === 'pending_whisper') url += '&whisper_local_status=pending';
                if (filter === 'pending_openai') url += '&whisper_openai_status=pending';
                if (filter === 'processed_asr') url += '&asr_status=processed';
                if (filter === 'processed_nolm') url += '&asr_nolm_status=processed';
                if (filter === 'processed_whisper') url += '&whisper_local_status=processed';
                if (filter === 'processed_openai') url += '&whisper_openai_status=processed';

                // Custom filters
                if (werOp && werValue) {
                    url += `&wer_op=${werOp}&wer_value=${werValue}`;
                }
                if (durOp && durValue) {
                    url += `&dur_op=${durOp}&dur_value=${durValue}`;
                }
                if (selectedSpeaker) {
                    url += `&speaker=${selectedSpeaker}`;
                }

                // Text search filter
                const filterText = document.getElementById('filter-text')?.value?.trim();
                if (filterText) {
                    url += `&text=${encodeURIComponent(filterText)}`;
                }

                // Noise level filter
                const filterNoise = document.getElementById('filter-noise')?.value;
                if (filterNoise) {
                    url += `&noise_level=${filterNoise}`;
                }

                // Chapter filter
                const filterChapter = document.getElementById('filter-chapter')?.value?.trim();
                if (filterChapter) {
                    url += `&chapter=${encodeURIComponent(filterChapter)}`;
                }

                const res = await fetch(url);
                const data = await res.json();

                if (data.success && data.data.files) {
                    loadedFiles = data.data.files; // <-- сохраняем
                    renderFiles(data.data.files);

                    const totalFiles = data.data.total || data.data.files.length;
                    totalPages = Math.ceil(totalFiles / currentLimit) || 1;

                    // Обновляем оба пагинатора
                    document.getElementById('current-page').textContent = currentPage;
                    document.getElementById('current-page-top').textContent = currentPage;
                    document.getElementById('total-pages').textContent = totalPages;
                    document.getElementById('total-pages-top').textContent = totalPages;
                    document.getElementById('filter-total').textContent = totalFiles;
                    document.getElementById('filter-total-top').textContent = totalFiles;

                    updatePagination();
                } else {
                    fileList.innerHTML = '<div class="col-span-2 text-center text-gray-500 py-8">No files found</div>';
                    totalPages = 1;

                    document.getElementById('current-page').textContent = 1;
                    document.getElementById('current-page-top').textContent = 1;
                    document.getElementById('total-pages').textContent = 1;
                    document.getElementById('total-pages-top').textContent = 1;
                    document.getElementById('filter-total').textContent = 0;
                    document.getElementById('filter-total-top').textContent = 0;

                    updatePagination();
                }
            } catch (e) {
                fileList.innerHTML = '<div class="col-span-2 text-center text-red-500 py-8">Error loading files</div>';
                console.error('Failed to load files:', e);
            } finally {
                fileList.classList.remove('loading');
            }
        }

        // ============================================================
        // RENDER FILES - добавлены NoLM, верификация, цветовая индикация
        // ============================================================

        function formatMetric(value, label) {
            if (value === undefined || value === null) {
                return `${label}:-`;
            }
            return `${label}:${(value * 100).toFixed(1)}%`;
        }

        function getAudioUrl(fileId) {
            const base = `${API_BASE}/api/audio/${fileId}`;
            return audioCacheBust[fileId] ? `${base}?t=${audioCacheBust[fileId]}` : base;
        }

        function renderFiles(files) {
            const fileList = document.getElementById('file-list');

            if (!files || files.length === 0) {
                fileList.innerHTML = '<div class="col-span-2 text-center text-gray-500 py-8">No files found</div>';
                return;
            }

            fileList.innerHTML = files.map(file => {
                // Определяем класс карточки по статусу
                const wer = file.wer || 0;
                const isVerified = file.operator_verified;
                const highWER = wer > 0.15;
                const needsReview = highWER && !isVerified;

                let cardClass = '';
                if (isVerified) {
                    cardClass = 'card-verified';
                } else if (needsReview) {
                    cardClass = 'card-needs-review';
                } else if (highWER) {
                    cardClass = 'card-high-wer';
                }

                return `
                <div class="bg-white rounded-lg shadow p-3 hover:shadow-md transition-shadow text-sm ${cardClass} relative">
                    <input type="checkbox" 
                        class="merge-checkbox absolute top-2 right-2 w-5 h-5 cursor-pointer"
                        data-file-id="${file.id}"
                        onchange="toggleMergeSelect(${file.id}, '${file.user_id}', ${file.duration_sec || 0}, \`${(file.transcription_original || '').replace(/`/g, '')}\`)"
                        ${selectedForMerge.some(f => f.id === file.id) ? 'checked' : ''}>
                    <div class="flex flex-wrap items-center gap-6 mb-2 leading-relaxed">
                        <span class="text-sm text-gray-500">#${file.id}</span>
                        <span class="text-sm font-mono bg-gray-100 px-2 py-0.5 rounded"><span class="font-semibold">Speaker:</span> ${file.user_id}</span>
                        <span class="text-sm font-mono bg-gray-100 px-2 py-0.5 rounded"><span class="font-semibold">Ch:</span> ${file.chapter_id}</span>
                        <span class="text-sm text-gray-600"><span class="font-semibold">Duration:</span> ${file.duration_sec?.toFixed(1) || '-'}s</span>
                        ${isVerified ? '<span class="bg-green-100 text-green-800 text-sm px-2 py-0.5 rounded-full">✓ Verified</span>' : ''}
                        ${file.original_edited ? '<span class="bg-yellow-100 text-yellow-800 text-sm px-2 py-0.5 rounded">(edited)</span>' : ''}
                    </div>
                    <div class="flex flex-wrap items-center gap-6 mb-2 text-sm leading-relaxed">
                        ${file.snr_db > 0 ? `<span class="text-gray-600"><span class="font-semibold">SNR:</span> ${file.snr_db.toFixed(1)}</span>` : ''}
                        ${file.noise_level ? `<span class="px-2 py-0.5 rounded ${file.noise_level === 'low' ? 'bg-green-100 text-green-700' : file.noise_level === 'medium' ? 'bg-yellow-100 text-yellow-700' : file.noise_level === 'high' ? 'bg-orange-100 text-orange-700' : 'bg-red-100 text-red-700'}"><span class="font-semibold">Noise:</span> ${file.noise_level}</span>` : ''}
                        ${file.snr_sox > 0 ? `<span class="text-gray-600"><span class="font-semibold">Sox:</span> ${file.snr_sox.toFixed(1)}</span>` : ''}
                        ${file.snr_wada > 0 ? `<span class="text-gray-600"><span class="font-semibold">WADA:</span> ${file.snr_wada.toFixed(1)}</span>` : ''}
                        ${file.snr_spectral > 0 ? `<span class="text-gray-600"><span class="font-semibold">Spec:</span> ${file.snr_spectral.toFixed(1)}</span>` : ''}
                        ${file.rms_db ? `<span class="text-gray-600"><span class="font-semibold">RMS:</span> ${file.rms_db.toFixed(1)}dB</span>` : ''}
                    </div>
                    <div class="flex flex-wrap items-center gap-6 mb-2 text-sm leading-relaxed">
                        <span><span class="font-semibold text-gray-600">ASR:</span> ${getStatusBadge(file.asr_status)}</span>
                        <span><span class="font-semibold text-gray-600">NoLM:</span> ${getStatusBadge(file.asr_nolm_status)}</span>
                        <span><span class="font-semibold text-gray-600">W.L:</span> ${getStatusBadge(file.whisper_local_status)}</span>
                        <span><span class="font-semibold text-gray-600">W.O:</span> ${getStatusBadge(file.whisper_openai_status)}</span>
                    </div>
                    
                    <audio controls preload="none" class="w-full mb-2">
                        <source src="${getAudioUrl(file.id)}" type="audio/wav">
                    </audio>

                    <div class="space-y-2 text-sm">
                        <div>
                            <span class="font-semibold text-gray-600">Orig:</span>
                            <span class="text-base">${file.transcription_original || '-'}</span>
                        </div>
                        
                        <div>
                            <span class="font-semibold text-blue-600">Kaldi:</span>
                            <span class="text-base">${file.asr_status === 'processed' ? highlightDiff(file.transcription_original, file.transcription_asr) : ''}</span>
                            <span class="text-gray-400 ml-1 text-xs">${file.asr_status === 'processed' ? formatMetric(file.wer, 'WER') : 'WER:-'}</span>
                        </div>

                        <div>
                            <span class="font-semibold text-indigo-600">NoLM:</span>
                            <span class="text-base">${file.asr_nolm_status === 'processed' ? highlightDiff(file.transcription_original, file.transcription_asr_nolm) : ''}</span>
                            <span class="text-gray-400 ml-1 text-xs">${file.asr_nolm_status === 'processed' ? formatMetric(file.wer_nolm, 'WER') : 'WER:-'}</span>
                        </div>
                        
                        <div>
                            <span class="font-semibold text-green-600">W.Local:</span>
                            <span class="text-base">${file.whisper_local_status === 'processed' ? highlightDiff(file.transcription_original, file.transcription_whisper_local) : ''}</span>
                            <span class="text-gray-400 ml-1 text-xs">${file.whisper_local_status === 'processed' ? formatMetric(file.wer_whisper_local, 'WER') : 'WER:-'}</span>
                        </div>
                        
                        <div>
                            <span class="font-semibold text-purple-600">W.OpenAI:</span>
                            <span class="text-base">${file.whisper_openai_status === 'processed' ? highlightDiff(file.transcription_original, file.transcription_whisper_openai) : ''}</span>
                            <span class="text-gray-400 ml-1 text-xs">${file.whisper_openai_status === 'processed' ? formatMetric(file.wer_whisper_openai, 'WER') : 'WER:-'}</span>
                        </div>
                    </div>

                    <div class="mt-2 flex flex-wrap gap-1">
                        <button onclick="showDetail(${file.id})" class="text-xs text-blue-500 hover:text-blue-700">Details</button>
                        <span class="text-gray-300">|</span>
                        ${!isVerified
                        ? `<button onclick="verifyFile(${file.id})" class="text-xs text-green-600 hover:text-green-800">✓ Verify</button>`
                        : `<button onclick="unverifyFile(${file.id})" class="text-xs text-gray-500 hover:text-gray-700">↩ Unverify</button>`
                    }
                        <span class="text-gray-300">|</span>
                        <button onclick="processFile(${file.id}, 'kaldi')" class="text-xs text-blue-500 hover:text-blue-700" title="Process with Kaldi">🔊 Kaldi</button>
                        <button onclick="processFile(${file.id}, 'kaldi-nolm')" class="text-xs text-indigo-500 hover:text-indigo-700" title="Process with Kaldi NoLM">🎯 NoLM</button>
                        <button onclick="processFile(${file.id}, 'whisper-local')" class="text-xs text-green-500 hover:text-green-700" title="Process with Whisper Local">🎤 Whisper</button>
                        <button onclick="processFile(${file.id}, 'whisper-openai')" class="text-xs text-purple-500 hover:text-purple-700" title="Process with OpenAI">💰 OpenAI</button>
                        <span class="text-gray-300">|</span>
                        <button onclick="recalcWER(${file.id})" class="text-xs text-orange-500 hover:text-orange-700" title="Recalculate WER/CER">🔄 Recalc</button>
                        <button onclick="analyzeFile(${file.id})" class="text-xs text-cyan-500 hover:text-cyan-700" title="Analyze SNR/RMS">📊 Analyze</button>
                        <button onclick="deleteFile(${file.id})" class="text-xs text-red-500 hover:text-red-700" title="Delete file">🗑️ Delete</button>
                    </div>
                </div>
            `}).join('');
        }

        function getStatusBadge(status) {
            const badges = {
                'processed': '<span class="bg-green-100 text-green-800 text-xs px-1 rounded">✓</span>',
                'pending': '<span class="bg-yellow-100 text-yellow-800 text-xs px-1 rounded">⏳</span>',
                'error': '<span class="bg-red-100 text-red-800 text-xs px-1 rounded">✗</span>'
            };
            return badges[status] || '<span class="text-xs text-gray-400">-</span>';
        }

        function highlightDiff(original, hypothesis) {
            if (!original || !hypothesis) return hypothesis || '';
            const origWords = original.toLowerCase().split(/\s+/);
            const hypWords = hypothesis.split(/\s+/);
            return hypWords.map(word => {
                const wordLower = word.toLowerCase().replace(/[.,!?]/g, '');
                if (!origWords.includes(wordLower)) {
                    return `<span class="diff-error px-0.5 rounded">${word}</span>`;
                }
                return word;
            }).join(' ');
        }

        async function analyzeFile(fileId) {
            try {
                const res = await fetch(`${API_BASE}/api/files/${fileId}/analyze`, { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    showToast(`SNR: ${data.data.snr_db?.toFixed(1)} dB, Noise: ${data.data.noise_level}`);
                    loadFiles();
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        // ============================================================
        // PAGINATION
        // ============================================================

        function updatePagination() {
            // Bottom pagination
            document.getElementById('current-page').textContent = currentPage;
            document.getElementById('total-pages').textContent = totalPages;
            document.getElementById('btn-first').disabled = currentPage <= 1;
            document.getElementById('btn-prev').disabled = currentPage <= 1;
            document.getElementById('btn-next').disabled = currentPage >= totalPages;
            document.getElementById('btn-last').disabled = currentPage >= totalPages;

            // Top pagination
            document.getElementById('current-page-top').textContent = currentPage;
            document.getElementById('total-pages-top').textContent = totalPages;
            document.getElementById('btn-first-top').disabled = currentPage <= 1;
            document.getElementById('btn-prev-top').disabled = currentPage <= 1;
            document.getElementById('btn-next-top').disabled = currentPage >= totalPages;
            document.getElementById('btn-last-top').disabled = currentPage >= totalPages;
        }

        function firstPage() {
            if (currentPage > 1) { currentPage = 1; loadFiles(); }
        }

        function prevPage() {
            if (currentPage > 1) { currentPage--; loadFiles(); }
        }

        function nextPage() {
            if (currentPage < totalPages) { currentPage++; loadFiles(); }
        }

        function lastPage() {
            if (currentPage < totalPages) { currentPage = totalPages; loadFiles(); }
        }

        // ============================================================
        // DETAIL MODAL - обновлён с редактированием и верификацией
        // ============================================================

        async function showDetail(id) {
            const modal = document.getElementById('modal');
            const content = document.getElementById('modal-content');

            try {
                const res = await fetch(`${API_BASE}/api/files/${id}`);
                const data = await res.json();

                if (data.success && data.data) {
                    const file = data.data;
                    content.innerHTML = `
                        <div class="space-y-4">
                            <div class="bg-gray-50 p-3 rounded">
                                <audio controls class="w-full">
                                    <source src="${getAudioUrl(file.id)}" type="audio/wav">
                                </audio>
                            </div>

                            <!-- Segments (Pyannote) -->
                            <div id="segments-container" class="mt-4"></div>

                            <div class="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                                <div><span class="text-gray-500">ID:</span> ${file.id}</div>
                                <div><span class="text-gray-500">User:</span> ${file.user_id}</div>
                                <div><span class="text-gray-500">Chapter:</span> ${file.chapter_id}</div>
                                <div><span class="text-gray-500">Duration:</span> ${file.duration_sec?.toFixed(2)}s</div>
                                <div><span class="text-gray-500">Sample Rate:</span> ${file.sample_rate} Hz</div>
                                <div><span class="text-gray-500">Channels:</span> ${file.channels}</div>
                                <div><span class="text-gray-500">Verified:</span> ${file.operator_verified ? '<span class="text-green-600">✓ Yes</span>' : 'No'}</div>
                                <div><span class="text-gray-500">Edited:</span> ${file.original_edited ? 'Yes' : 'No'}</div>
                            </div>

                            <div class="space-y-2">
                                <h3 class="font-bold text-gray-700">Transcriptions</h3>
                                
                                <!-- Editable Original -->
                                <div class="bg-gray-50 p-3 rounded">
                                    <div class="font-semibold text-gray-700 mb-1">
                                        Original ${file.original_edited ? '<span class="text-yellow-600">(edited)</span>' : ''}
                                    </div>
                                    <textarea id="edit-orig-${file.id}" 
                                        class="w-full border rounded p-2 text-sm" 
                                        rows="2">${file.transcription_original || ''}</textarea>
                                    <button onclick="saveTranscription(${file.id})" 
                                        class="mt-2 bg-blue-500 text-white px-3 py-1 rounded text-sm hover:bg-blue-600">
                                        💾 Save & Recalc WER
                                    </button>
                                </div>

                                <div class="bg-blue-50 p-3 rounded">
                                    <div class="flex justify-between items-center mb-1">
                                        <span class="font-semibold text-blue-700">Kaldi ASR</span>
                                        <span class="text-sm">WER: ${((file.wer || 0) * 100).toFixed(2)}% | CER: ${((file.cer || 0) * 100).toFixed(2)}%</span>
                                    </div>
                                    <div class="text-gray-800 text-base">${file.transcription_asr || '<span class="text-gray-400">Not processed</span>'}</div>
                                </div>

                                <div class="bg-indigo-50 p-3 rounded">
                                    <div class="flex justify-between items-center mb-1">
                                        <span class="font-semibold text-indigo-700">Kaldi NoLM</span>
                                        <span class="text-sm">WER: ${((file.wer_nolm || 0) * 100).toFixed(2)}% | CER: ${((file.cer_nolm || 0) * 100).toFixed(2)}%</span>
                                    </div>
                                    <div class="text-gray-800 text-base">${file.transcription_asr_nolm || '<span class="text-gray-400">Not processed</span>'}</div>
                                </div>

                                <div class="bg-green-50 p-3 rounded">
                                    <div class="flex justify-between items-center mb-1">
                                        <span class="font-semibold text-green-700">Whisper Local</span>
                                        <span class="text-sm">WER: ${((file.wer_whisper_local || 0) * 100).toFixed(2)}% | CER: ${((file.cer_whisper_local || 0) * 100).toFixed(2)}%</span>
                                    </div>
                                    <div class="text-gray-800 text-base">${file.transcription_whisper_local || '<span class="text-gray-400">Not processed</span>'}</div>
                                </div>

                                <div class="bg-purple-50 p-3 rounded">
                                    <div class="flex justify-between items-center mb-1">
                                        <span class="font-semibold text-purple-700">Whisper OpenAI</span>
                                        <span class="text-sm">WER: ${((file.wer_whisper_openai || 0) * 100).toFixed(2)}% | CER: ${((file.cer_whisper_openai || 0) * 100).toFixed(2)}%</span>
                                    </div>
                                    <div class="text-gray-800 text-base">${file.transcription_whisper_openai || '<span class="text-gray-400">Not processed</span>'}</div>
                                </div>
                            </div>

                            <!-- Verification buttons -->
                            <div class="flex gap-2">
                                ${!file.operator_verified
                            ? `<button onclick="verifyFile(${file.id}); closeModal();" 
                                        class="bg-green-500 text-white px-4 py-2 rounded hover:bg-green-600">
                                        ✓ Mark as Verified
                                       </button>`
                            : `<button onclick="unverifyFile(${file.id}); closeModal();" 
                                        class="bg-gray-400 text-white px-4 py-2 rounded hover:bg-gray-500">
                                        ↩ Remove Verification
                                       </button>`
                        }
                            </div>

                            <div class="text-xs text-gray-500 break-all">
                                <span class="font-semibold">Path:</span> ${file.file_path}
                            </div>
                        </div>
                    `;
                    modal.classList.remove('hidden');
                    modal.classList.add('flex');

                    // Init segments
                    initSegments(file.id);
                }
            } catch (e) {
                console.error('Failed to load file details:', e);
            }
        }

        function closeModal() {
            const modal = document.getElementById('modal');
            modal.classList.add('hidden');
            modal.classList.remove('flex');
        }

        document.getElementById('modal').addEventListener('click', function (e) {
            if (e.target === this) closeModal();
        });

        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') closeModal();
        });

        // ============================================================
        // RECALC WER
        // ============================================================

        async function recalcWER(fileId) {
            try {
                const res = await fetch(`${API_BASE}/api/recalc/${fileId}`, { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    loadFiles();
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        async function recalcAll() {
            if (!confirm('Recalculate WER/CER for all processed files?')) return;

            try {
                const res = await fetch(`${API_BASE}/api/recalc-all`, { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    alert(`Recalculated: ${data.data.count} files`);
                    loadStats();
                    loadFiles();
                } else {
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                alert(`Error: ${e.message}`);
            }
        }

        // ============================================================
        // SPEAKERS
        // ============================================================

        async function loadSpeakers() {
            try {
                const res = await fetch(`${API_BASE}/api/speakers`);
                const data = await res.json();
                if (data.success) {
                    allSpeakers = data.data || [];
                    document.getElementById('stat-speakers').textContent = allSpeakers.length;
                }
            } catch (e) {
                console.error('Failed to load speakers:', e);
            }
        }

        function showSpeakerDropdown(filter = '') {
            const dropdown = document.getElementById('speaker-dropdown');
            const filtered = filter
                ? allSpeakers.filter(s => s.startsWith(filter))
                : allSpeakers.slice(0, 50);

            if (filtered.length === 0) {
                dropdown.classList.add('hidden');
                return;
            }

            dropdown.innerHTML = filtered.slice(0, 20).map(s => `
                <div class="px-3 py-1 hover:bg-blue-100 cursor-pointer text-sm" 
                    onclick="selectSpeaker('${s}')">${s}</div>
            `).join('');

            if (filtered.length > 20) {
                dropdown.innerHTML += `<div class="px-3 py-1 text-gray-400 text-xs">...and ${filtered.length - 20} more</div>`;
            }

            dropdown.classList.remove('hidden');
        }

        function selectSpeaker(speaker) {
            selectedSpeaker = speaker;
            document.getElementById('speaker-search').value = speaker;
            document.getElementById('speaker-dropdown').classList.add('hidden');
            document.getElementById('btn-clear-speaker').classList.remove('hidden');
            currentPage = 1;
            loadFiles();
        }

        function clearSpeaker() {
            selectedSpeaker = '';
            document.getElementById('speaker-search').value = '';
            document.getElementById('btn-clear-speaker').classList.add('hidden');
            currentPage = 1;
            loadFiles();
        }
        
        // ============================================================
        // CLIENT-SIDE SORTING
        // ============================================================

        let loadedFiles = []; // хранит загруженные файлы
        let currentSort = { field: 'id', order: 'desc' };

        function sortFiles(field) {
            // Переключаем порядок если тот же field
            if (currentSort.field === field) {
                currentSort.order = currentSort.order === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.field = field;
                currentSort.order = 'asc';
            }

            const sorted = [...loadedFiles].sort((a, b) => {
                let valA = a[field];
                let valB = b[field];

                // Для строк
                if (typeof valA === 'string') {
                    valA = valA.toLowerCase();
                    valB = (valB || '').toLowerCase();
                }

                // Для null/undefined
                if (valA == null) valA = 0;
                if (valB == null) valB = 0;

                let result = 0;
                if (valA < valB) result = -1;
                if (valA > valB) result = 1;

                return currentSort.order === 'asc' ? result : -result;
            });

            renderFiles(sorted);
            updateSortButtons();
        }

        function updateSortButtons() {
            document.querySelectorAll('.sort-btn').forEach(btn => {
                const field = btn.dataset.field;
                btn.classList.remove('bg-blue-500', 'text-white', 'bg-gray-200');

                if (field === currentSort.field) {
                    btn.classList.add('bg-blue-500', 'text-white');
                    btn.textContent = btn.dataset.label + (currentSort.order === 'asc' ? ' ↑' : ' ↓');
                } else {
                    btn.classList.add('bg-gray-200');
                    btn.textContent = btn.dataset.label;
                }
            });
        }

        function showToast(message, isError = false) {
            const toast = document.createElement('div');
            toast.className = `fixed bottom-4 right-4 px-4 py-2 rounded shadow-lg z-50 ${isError ? 'bg-red-500' : 'bg-green-500'} text-white`;
            toast.textContent = message;
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 3000);
        }

        // Quick ID search
        function searchById() {
            const searchId = document.getElementById('filter-id')?.value?.trim();
            if (!searchId) {
                loadFiles();
                return;
            }
            
            // Попробуем загрузить напрямую
            fetch(`/api/files/${searchId}`)
                .then(r => r.json())
                .then(data => {
                    if (data.success && data.data) {
                        loadedFiles = [data.data];
                        renderFiles([data.data]);
                        document.getElementById('current-page').textContent = 1;
                        document.getElementById('current-page-top').textContent = 1;
                        document.getElementById('total-pages').textContent = 1;
                        document.getElementById('total-pages-top').textContent = 1;
                        document.getElementById('filter-total').textContent = 1;
                        document.getElementById('filter-total-top').textContent = 1;
                    } else {
                        document.getElementById('file-list').innerHTML = 
                            '<div class="col-span-2 text-center text-gray-500 py-8">File #' + searchId + ' not found</div>';
                    }
                })
                .catch(e => {
                    document.getElementById('file-list').innerHTML = 
                        '<div class="col-span-2 text-center text-red-500 py-8">Error: ' + e.message + '</div>';
                });
        }

        // ============================================================
        // EVENT LISTENERS
        // ============================================================

        document.getElementById('speaker-search').addEventListener('input', function (e) {
            showSpeakerDropdown(e.target.value);
        });

        document.getElementById('speaker-search').addEventListener('focus', function (e) {
            showSpeakerDropdown(e.target.value);
        });

        document.addEventListener('click', function (e) {
            if (!e.target.closest('#speaker-search') && !e.target.closest('#speaker-dropdown')) {
                document.getElementById('speaker-dropdown').classList.add('hidden');
            }
        });

        document.getElementById('filter').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('filter-verified').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('filter-merged').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('filter-active')?.addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('limit').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('wer-op').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('wer-value').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('dur-op').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('dur-value').addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('filter-noise')?.addEventListener('change', function () {
            currentPage = 1;
            loadFiles();
        });

        document.getElementById('filter-text')?.addEventListener('keypress', function (e) {
            if (e.key === 'Enter') {
                currentPage = 1;
                loadFiles();
            }
        });

        document.getElementById('filter-chapter')?.addEventListener('keypress', function (e) {
            if (e.key === 'Enter') {
                currentPage = 1;
                loadFiles();
            }
        });
        
        // ============================================================
        // INITIAL LOAD
        // ============================================================

        loadSpeakers();
        loadStats();
        loadFiles();

        // Auto-refresh stats every 30 seconds
        setInterval(loadStats, 30000);