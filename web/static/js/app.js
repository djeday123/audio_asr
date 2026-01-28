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
