

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
// VERIFICATION
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

// ============================================================
// DELETE FILE
// ============================================================

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
// RENDER FILES - добавлены NoLM, верификация, цветовая индикация
// ============================================================

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