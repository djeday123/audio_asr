// ============================================================
// MERGE QUEUE
// ============================================================

let mergeStatusInterval = null;

async function addToMergeQueue() {
    const input = document.getElementById('merge-input');
    const lines = input.value.trim();
    
    if (!lines) {
        showMergeResult('Please enter IDs to merge', 'warning');
        return;
    }
    
    try {
        const resp = await fetch('/api/merge/queue/batch', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({lines: lines})
        });
        
        const data = await resp.json();
        
        if (data.success) {
            const d = data.data;
            let html = `<strong>Added:</strong> ${d.added} | <strong>Skipped:</strong> ${d.skipped} | <strong>Errors:</strong> ${d.errors}`;
            
            if (d.results && d.results.length > 0) {
                html += '<ul class="mt-2 text-xs space-y-1">';
                for (const r of d.results) {
                    let icon = '✓';
                    let cls = 'text-green-600';
                    if (r.status === 'skipped') {
                        icon = '⊘';
                        cls = 'text-yellow-600';
                    } else if (r.status === 'error') {
                        icon = '✗';
                        cls = 'text-red-600';
                    }
                    
                    let detail = r.error || r.reason || `queue_id: ${r.queue_id}`;
                    
                    // Показываем warnings
                    if (r.warnings && r.warnings.length > 0) {
                        detail += ` <span class="text-orange-500">(⚠ ${r.warnings.join(', ')})</span>`;
                    }
                    
                    html += `<li class="${cls}">${icon} <code>${r.ids}</code> — ${detail}</li>`;
                }
                html += '</ul>';
            }
            
            showMergeResult(html, d.errors > 0 ? 'warning' : 'success');
            
            // Очищаем только если всё успешно
            if (d.added > 0 && d.errors === 0 && d.skipped === 0) {
                input.value = '';
            }
            
            refreshMergeQueue();
        } else {
            showMergeResult(data.error, 'error');
        }
    } catch (e) {
        showMergeResult('Request failed: ' + e.message, 'error');
    }
}

async function startMergeQueue() {
    try {
        const resp = await fetch('/api/merge/queue/start', {method: 'POST'});
        const data = await resp.json();
        
        if (data.success) {
            showMergeResult('Queue processing started', 'success');
            pollMergeStatus();
        } else {
            showMergeResult(data.error, 'error');
        }
    } catch (e) {
        showMergeResult('Failed to start: ' + e.message, 'error');
    }
}

async function stopMergeQueue() {
    try {
        await fetch('/api/merge/queue/stop', {method: 'POST'});
        showMergeResult('Queue stopped', 'info');
        if (mergeStatusInterval) {
            clearInterval(mergeStatusInterval);
            mergeStatusInterval = null;
        }
        refreshMergeQueue();
    } catch (e) {
        showMergeResult('Failed to stop: ' + e.message, 'error');
    }
}

async function refreshMergeQueue() {
    // Status
    try {
        const statusResp = await fetch('/api/merge/queue/status');
        const statusData = await statusResp.json();
        
        if (statusData.success) {
            const s = statusData.data;
            const badge = document.getElementById('merge-status-badge');
            const statusDiv = document.getElementById('merge-status');
            
            if (!badge || !statusDiv) return;
            
            if (s.running) {
                badge.className = 'px-2 py-1 rounded text-xs font-semibold bg-green-500 text-white';
                badge.textContent = 'Running';
                const pct = s.total ? Math.round(s.processed / s.total * 100) : 0;
                statusDiv.innerHTML = `
                    <div class="flex justify-between text-sm">
                        <span>Progress:</span>
                        <strong>${s.processed} / ${s.total}</strong>
                    </div>
                    <div class="w-full bg-gray-200 rounded h-2 mt-1">
                        <div class="bg-green-500 h-2 rounded" style="width: ${pct}%"></div>
                    </div>
                    <div class="text-xs text-gray-500 mt-1">Errors: ${s.errors} | Elapsed: ${s.elapsed}</div>
                `;
                
                // Запускаем polling если ещё не запущен
                if (!mergeStatusInterval) {
                    pollMergeStatus();
                }
            } else {
                badge.className = 'px-2 py-1 rounded text-xs font-semibold bg-gray-200 text-gray-700';
                badge.textContent = 'Idle';
                statusDiv.innerHTML = `<span class="text-gray-600 text-sm">Last run: ${s.processed} processed, ${s.errors} errors</span>`;
            }
        }
    } catch (e) {
        console.error('Merge status error:', e);
    }
    
    // Queue list
    try {
        const queueResp = await fetch('/api/merge/queue?limit=20');
        const queueData = await queueResp.json();
        
        if (queueData.success && queueData.data.items) {
            const list = document.getElementById('merge-queue-list');
            if (!list) return;
            
            if (queueData.data.items.length === 0) {
                list.innerHTML = '<div class="p-3 text-gray-500 text-sm">No items in queue</div>';
            } else {
                list.innerHTML = queueData.data.items.map(item => {
                    let badgeClass = 'bg-gray-200 text-gray-700';
                    let badgeText = item.status;
                    
                    if (item.status === 'completed') {
                        badgeClass = 'bg-green-100 text-green-700';
                        badgeText = '✓ done';
                    } else if (item.status === 'error') {
                        badgeClass = 'bg-red-100 text-red-700';
                        badgeText = '✗ error';
                    } else if (item.status === 'processing') {
                        badgeClass = 'bg-blue-100 text-blue-700';
                        badgeText = '⏳ processing';
                    } else if (item.status === 'pending') {
                        badgeClass = 'bg-yellow-100 text-yellow-700';
                        badgeText = '⋯ pending';
                    }
                    
                    let detail = '';
                    if (item.merged_file_id) {
                        detail = `<span class="text-green-600">→ ID: ${item.merged_file_id} (${item.merged_duration?.toFixed(1)}s)</span>`;
                    } else if (item.error_message) {
                        detail = `<span class="text-red-500 text-xs">${item.error_message}</span>`;
                    }
                    
                    return `
                        <div class="border-b last:border-0 px-3 py-2 hover:bg-gray-50">
                            <div class="flex justify-between items-center">
                                <code class="text-xs text-gray-700">${item.ids_string}</code>
                                <div class="flex items-center gap-2">
                                    <span class="px-2 py-0.5 rounded text-xs ${badgeClass}">${badgeText}</span>
                                    <button onclick="deleteMergeQueueItem(${item.id})" 
                                        class="text-gray-400 hover:text-red-500 text-sm" title="Delete">✕</button>
                                </div>
                            </div>
                            ${detail ? `<div class="mt-1 text-sm">${detail}</div>` : ''}
                        </div>
                    `;
                }).join('');
            }
        }
    } catch (e) {
        console.error('Merge queue list error:', e);
    }
}

function pollMergeStatus() {
    if (mergeStatusInterval) {
        clearInterval(mergeStatusInterval);
    }
    
    mergeStatusInterval = setInterval(async () => {
        await refreshMergeQueue();
        
        // Проверяем, закончилась ли обработка
        try {
            const resp = await fetch('/api/merge/queue/status');
            const data = await resp.json();
            if (data.success && !data.data.running) {
                clearInterval(mergeStatusInterval);
                mergeStatusInterval = null;
                // Обновляем основной список файлов
                if (typeof loadFiles === 'function') {
                    loadFiles();
                }
                if (typeof loadStats === 'function') {
                    loadStats();
                }
            }
        } catch (e) {
            console.error('Poll error:', e);
        }
    }, 2000);
}

function showMergeResult(message, type) {
    const div = document.getElementById('merge-result');
    if (!div) return;
    
    let bgClass = 'bg-blue-50 text-blue-700 border-blue-200';
    
    if (type === 'success') {
        bgClass = 'bg-green-50 text-green-700 border-green-200';
    } else if (type === 'error') {
        bgClass = 'bg-red-50 text-red-700 border-red-200';
    } else if (type === 'warning') {
        bgClass = 'bg-yellow-50 text-yellow-700 border-yellow-200';
    } else if (type === 'info') {
        bgClass = 'bg-blue-50 text-blue-700 border-blue-200';
    }
    
    div.innerHTML = `<div class="border rounded p-2 text-sm ${bgClass}">${message}</div>`;
    
    // Auto-hide для success
    if (type === 'success') {
        setTimeout(() => {
            if (div.innerHTML.includes(message)) {
                div.innerHTML = '';
            }
        }, 10000);
    }
}

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', () => {
    // Проверяем что элементы существуют
    if (document.getElementById('merge-input')) {
        refreshMergeQueue();
    }
});

// ============================================================
// MERGE FUNCTIONALITY
// ============================================================

let selectedForMerge = []; // [{id, speaker, duration, transcription}, ...]

function toggleMergeSelect(fileId, speaker, duration, transcription) {
    const idx = selectedForMerge.findIndex(f => f.id === fileId);

    if (idx >= 0) {
        // Убираем из выбранных
        selectedForMerge.splice(idx, 1);
    } else {
        // Проверяем спикера
        if (selectedForMerge.length > 0 && selectedForMerge[0].speaker !== speaker) {
            alert('Все файлы должны быть от одного спикера!');
            return;
        }
        // Добавляем
        selectedForMerge.push({ id: fileId, speaker, duration, transcription });
    }

    updateMergePanel();
    updateCheckboxes();
}

function updateMergePanel() {
    const panel = document.getElementById('merge-panel');
    const list = document.getElementById('merge-list');

    if (selectedForMerge.length === 0) {
        panel.classList.add('hidden');
        return;
    }

    panel.classList.remove('hidden');

    document.getElementById('merge-count').textContent = selectedForMerge.length;
    document.getElementById('merge-speaker').textContent = selectedForMerge[0]?.speaker || '-';

    const totalDur = selectedForMerge.reduce((sum, f) => sum + f.duration, 0);
    document.getElementById('merge-duration').textContent = totalDur.toFixed(2);

    // Рендерим список с кнопками перемещения
    list.innerHTML = selectedForMerge.map((f, idx) => `
        <div class="flex items-center gap-2 p-2 border-b last:border-0 hover:bg-gray-50">
            <span class="text-gray-400 w-6">${idx + 1}.</span>
            <div class="flex-1">
                <span class="font-mono text-sm">#${f.id}</span>
                <span class="text-gray-500 text-xs ml-2">${f.duration.toFixed(2)}s</span>
                <div class="text-xs text-gray-600 truncate max-w-md">${f.transcription || '-'}</div>
            </div>
            <div class="flex gap-1">
                <button onclick="moveMergeItem(${idx}, -1)" 
                    class="text-gray-400 hover:text-blue-500 ${idx === 0 ? 'invisible' : ''}"
                    title="Move up">↑</button>
                <button onclick="moveMergeItem(${idx}, 1)" 
                    class="text-gray-400 hover:text-blue-500 ${idx === selectedForMerge.length - 1 ? 'invisible' : ''}"
                    title="Move down">↓</button>
                <button onclick="removeMergeItem(${idx})" 
                    class="text-gray-400 hover:text-red-500"
                    title="Remove">✕</button>
            </div>
        </div>
    `).join('');
}

function moveMergeItem(idx, direction) {
    const newIdx = idx + direction;
    if (newIdx < 0 || newIdx >= selectedForMerge.length) return;

    // Swap
    [selectedForMerge[idx], selectedForMerge[newIdx]] =
        [selectedForMerge[newIdx], selectedForMerge[idx]];

    updateMergePanel();
}

function removeMergeItem(idx) {
    selectedForMerge.splice(idx, 1);
    updateMergePanel();
    updateCheckboxes();
}

function clearMergeSelection() {
    selectedForMerge = [];
    updateMergePanel();
    updateCheckboxes();
}

function closeMergePanel() {
    document.getElementById('merge-panel').classList.add('hidden');
}

function updateCheckboxes() {
    document.querySelectorAll('.merge-checkbox').forEach(cb => {
        const fileId = parseInt(cb.dataset.fileId);
        cb.checked = selectedForMerge.some(f => f.id === fileId);
    });
}

async function executeMerge() {
    if (selectedForMerge.length < 2) {
        alert('Выберите минимум 2 файла');
        return;
    }

    const ids = selectedForMerge.map(f => f.id);

    try {
        const res = await fetch('/api/merge', {  // <-- убрал API_BASE
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ids })
        });

        const data = await res.json();

        if (data.success) {
            alert(`Merged! New file ID: ${data.data.new_id}\nPath: ${data.data.output_path}`);
            clearMergeSelection();
            loadFiles();
            loadStats();
        } else {
            alert(`Error: ${data.error}`);
        }
    } catch (e) {
        alert(`Error: ${e.message}`);
    }
}

// Удалить один item
async function deleteMergeQueueItem(id) {
    try {
        const resp = await fetch(`/api/merge/queue/${id}`, {method: 'DELETE'});
        const data = await resp.json();
        if (data.success) {
            refreshMergeQueue();
        } else {
            alert('Error: ' + data.error);
        }
    } catch (e) {
        alert('Error: ' + e.message);
    }
}

// Очистить очередь
async function clearMergeQueue(status = 'pending') {
    const statusText = status === 'all' ? 'ALL items' : `${status} items`;
    if (!confirm(`Clear ${statusText} from queue?`)) return;
    
    try {
        const resp = await fetch(`/api/merge/queue/clear?status=${status}`, {method: 'DELETE'});
        const data = await resp.json();
        if (data.success) {
            showMergeResult(`Cleared ${data.data.cleared} items`, 'success');
            refreshMergeQueue();
        } else {
            showMergeResult('Error: ' + data.error, 'error');
        }
    } catch (e) {
        showMergeResult('Error: ' + e.message, 'error');
    }
}