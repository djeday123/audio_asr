/**
 * Segment Manager - с регулировкой границ на waveform
 */
class SegmentManager {
    constructor(fileId) {
        this.fileId = fileId;
        this.segments = [];
        this.groups = [];
        this.audioElement = null;
        this.audioContext = null;
        this.audioBuffer = null;
        this.duration = 0;
        
        // Drag state
        this.dragging = null; // {groupIdx, edge: 'start'|'end'}
        this.canvas = null;
    }

    async diarize() {
        const btn = document.getElementById('btn-diarize');
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Analyzing...';
        }

        try {
            const resp = await fetch(`/api/files/${this.fileId}/diarize`, { method: 'POST' });
            const data = await resp.json();
            
            if (data.success) {
                showToast(`Found ${data.data.segments} segments, ${data.data.num_speakers} speakers`);
                await this.loadSegments();
            } else {
                showToast('Error: ' + data.error, true);
            }
        } catch (e) {
            showToast('Diarize error: ' + e.message, true);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.textContent = '🎯 Split Audio';
            }
        }
    }

    async loadSegments() {
        try {
            const resp = await fetch(`/api/files/${this.fileId}/segments`);
            const data = await resp.json();
            if (data.success) {
                this.segments = data.data || [];
                // Группы с кастомными границами
                this.groups = this.segments.map(seg => ({
                    segments: [seg],
                    transcript: seg.transcript || '',
                    customStart: null, // null = использовать сегмент
                    customEnd: null
                }));
                this.render();
                this.loadAudioForWaveform();
            }
        } catch (e) {
            console.error('Load segments error:', e);
        }
    }

    async loadAudioForWaveform() {
        try {
            this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
            const resp = await fetch(`/api/audio/${this.fileId}`);
            const arrayBuffer = await resp.arrayBuffer();
            this.audioBuffer = await this.audioContext.decodeAudioData(arrayBuffer);
            this.duration = this.audioBuffer.duration;
            this.drawWaveform();
            this.setupCanvasEvents();
        } catch (e) {
            console.error('Load audio error:', e);
        }
    }

    getGroupStart(group) {
        if (group.customStart !== null) return group.customStart;
        return group.segments[0].start;
    }

    getGroupEnd(group) {
        if (group.customEnd !== null) return group.customEnd;
        return group.segments[group.segments.length - 1].end;
    }

    drawWaveform() {
        this.canvas = document.getElementById('waveform-canvas');
        if (!this.canvas || !this.audioBuffer) return;

        const ctx = this.canvas.getContext('2d');
        const width = this.canvas.width;
        const height = this.canvas.height;
        const data = this.audioBuffer.getChannelData(0);
        const duration = this.duration;

        // Clear
        ctx.fillStyle = '#1f2937';
        ctx.fillRect(0, 0, width, height);

        // Draw segments background
        this.groups.forEach((group, groupIdx) => {
            const startTime = this.getGroupStart(group);
            const endTime = this.getGroupEnd(group);
            const x1 = (startTime / duration) * width;
            const x2 = (endTime / duration) * width;
            
            const color = this.getGroupColor(groupIdx);
            ctx.fillStyle = color + '40';
            ctx.fillRect(x1, 0, x2 - x1, height);
            
            // Border
            ctx.strokeStyle = color;
            ctx.lineWidth = 2;
            ctx.strokeRect(x1, 0, x2 - x1, height);

            // Draggable handles
            ctx.fillStyle = color;
            // Left handle
            ctx.fillRect(x1 - 3, 0, 6, height);
            // Right handle
            ctx.fillRect(x2 - 3, 0, 6, height);
            
            // Group number
            ctx.fillStyle = '#fff';
            ctx.font = 'bold 12px sans-serif';
            ctx.fillText(`G${groupIdx + 1}`, x1 + 5, 15);
        });

        // Draw waveform
        ctx.beginPath();
        ctx.strokeStyle = '#60a5fa';
        ctx.lineWidth = 1;

        const step = Math.ceil(data.length / width);
        for (let i = 0; i < width; i++) {
            const idx = i * step;
            let min = 1.0, max = -1.0;
            for (let j = 0; j < step && idx + j < data.length; j++) {
                const val = data[idx + j];
                if (val < min) min = val;
                if (val > max) max = val;
            }
            const y1 = ((1 + min) / 2) * height;
            const y2 = ((1 + max) / 2) * height;
            ctx.moveTo(i, y1);
            ctx.lineTo(i, y2);
        }
        ctx.stroke();

        // Time markers
        ctx.fillStyle = '#9ca3af';
        ctx.font = '10px monospace';
        for (let t = 0; t <= duration; t += Math.ceil(duration / 10)) {
            const x = (t / duration) * width;
            ctx.fillText(this.formatTime(t), x + 2, height - 3);
        }
    }

    setupCanvasEvents() {
        if (!this.canvas) return;

        this.canvas.style.cursor = 'default';

        this.canvas.onmousedown = (e) => {
            const rect = this.canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const time = (x / this.canvas.width) * this.duration;
            
            // Find if clicking on a handle
            for (let i = 0; i < this.groups.length; i++) {
                const group = this.groups[i];
                const startTime = this.getGroupStart(group);
                const endTime = this.getGroupEnd(group);
                const startX = (startTime / this.duration) * this.canvas.width;
                const endX = (endTime / this.duration) * this.canvas.width;
                
                if (Math.abs(x - startX) < 8) {
                    this.dragging = { groupIdx: i, edge: 'start' };
                    this.canvas.style.cursor = 'ew-resize';
                    return;
                }
                if (Math.abs(x - endX) < 8) {
                    this.dragging = { groupIdx: i, edge: 'end' };
                    this.canvas.style.cursor = 'ew-resize';
                    return;
                }
            }
        };

        this.canvas.onmousemove = (e) => {
            const rect = this.canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            
            if (this.dragging) {
                const time = Math.max(0, Math.min(this.duration, (x / this.canvas.width) * this.duration));
                const group = this.groups[this.dragging.groupIdx];
                
                if (this.dragging.edge === 'start') {
                    const endTime = this.getGroupEnd(group);
                    if (time < endTime - 0.1) {
                        group.customStart = Math.round(time * 100) / 100;
                    }
                } else {
                    const startTime = this.getGroupStart(group);
                    if (time > startTime + 0.1) {
                        group.customEnd = Math.round(time * 100) / 100;
                    }
                }
                
                this.drawWaveform();
                this.updateGroupDisplay(this.dragging.groupIdx);
            } else {
                // Change cursor when hovering over handles
                let onHandle = false;
                for (let i = 0; i < this.groups.length; i++) {
                    const group = this.groups[i];
                    const startX = (this.getGroupStart(group) / this.duration) * this.canvas.width;
                    const endX = (this.getGroupEnd(group) / this.duration) * this.canvas.width;
                    
                    if (Math.abs(x - startX) < 8 || Math.abs(x - endX) < 8) {
                        onHandle = true;
                        break;
                    }
                }
                this.canvas.style.cursor = onHandle ? 'ew-resize' : 'default';
            }
        };

        this.canvas.onmouseup = () => {
            if (this.dragging) {
                this.dragging = null;
                this.canvas.style.cursor = 'default';
                this.render(); // Full re-render to update time displays
            }
        };

        this.canvas.onmouseleave = () => {
            if (this.dragging) {
                this.dragging = null;
                this.canvas.style.cursor = 'default';
            }
        };
    }

    updateGroupDisplay(groupIdx) {
        const group = this.groups[groupIdx];
        const timeEl = document.getElementById(`group-time-${groupIdx}`);
        const durEl = document.getElementById(`group-dur-${groupIdx}`);
        
        if (timeEl) {
            const start = this.getGroupStart(group);
            const end = this.getGroupEnd(group);
            timeEl.textContent = `${this.formatTime(start)} - ${this.formatTime(end)}`;
        }
        if (durEl) {
            const start = this.getGroupStart(group);
            const end = this.getGroupEnd(group);
            durEl.textContent = `(${(end - start).toFixed(1)}s)`;
        }
    }

    // Точная подстройка границ вводом - БЕЗ полного ре-рендера
    adjustBoundary(groupIdx, edge, delta) {
        const group = this.groups[groupIdx];
        
        if (edge === 'start') {
            const current = this.getGroupStart(group);
            const newVal = Math.max(0, current + delta);
            const endTime = this.getGroupEnd(group);
            if (newVal < endTime - 0.1) {
                group.customStart = Math.round(newVal * 100) / 100;
            }
        } else {
            const current = this.getGroupEnd(group);
            const newVal = Math.min(this.duration, current + delta);
            const startTime = this.getGroupStart(group);
            if (newVal > startTime + 0.1) {
                group.customEnd = Math.round(newVal * 100) / 100;
            }
        }
        
        // Сохраняем текущие транскрипции перед обновлением
        this.saveCurrentTranscriptsToMemory();
        
        // Только обновляем waveform и display, БЕЗ полного render
        this.drawWaveform();
        this.updateGroupDisplay(groupIdx);
        this.updateResetButton(groupIdx);
    }

    updateResetButton(groupIdx) {
        const group = this.groups[groupIdx];
        const hasCustom = group.customStart !== null || group.customEnd !== null;
        const resetBtn = document.getElementById(`reset-btn-${groupIdx}`);
        if (resetBtn) {
            resetBtn.style.display = hasCustom ? 'inline-block' : 'none';
        }
        
        // Обновляем badge
        let badge = document.getElementById(`adjusted-badge-${groupIdx}`);
        const header = document.querySelector(`#group-item-${groupIdx} .group-header .group-seg-count`);
        
        if (hasCustom && !badge && header) {
            const newBadge = document.createElement('span');
            newBadge.id = `adjusted-badge-${groupIdx}`;
            newBadge.className = 'custom-badge';
            newBadge.textContent = 'ADJUSTED';
            header.parentNode.insertBefore(newBadge, header);
        } else if (!hasCustom && badge) {
            badge.remove();
        }
    }

    saveCurrentTranscriptsToMemory() {
        this.groups.forEach((group, idx) => {
            const textarea = document.getElementById(`group-transcript-${idx}`);
            if (textarea) {
                group.transcript = textarea.value;
            }
        });
    }

    updateGroupBadges(groupIdx) {
        const group = this.groups[groupIdx];
        const hasCustom = group.customStart !== null || group.customEnd !== null;
        const badgeContainer = document.querySelector(`#group-item-${groupIdx} .group-badges`);
        
        // Обновляем ADJUSTED badge
        let adjustedBadge = document.getElementById(`adjusted-badge-${groupIdx}`);
        if (hasCustom && !adjustedBadge) {
            const header = document.querySelector(`#group-item-${groupIdx} .group-header`);
            if (header) {
                const badge = document.createElement('span');
                badge.id = `adjusted-badge-${groupIdx}`;
                badge.className = 'custom-badge';
                badge.textContent = 'ADJUSTED';
                header.insertBefore(badge, header.querySelector('.group-seg-count'));
            }
        } else if (!hasCustom && adjustedBadge) {
            adjustedBadge.remove();
        }
        
        // Показываем/скрываем кнопку reset
        const resetBtn = document.getElementById(`reset-btn-${groupIdx}`);
        if (resetBtn) {
            resetBtn.style.display = hasCustom ? 'inline-block' : 'none';
        }
    }

    // Сбросить к оригинальным границам
    resetBoundaries(groupIdx) {
        this.saveCurrentTranscriptsToMemory();

        const group = this.groups[groupIdx];
        group.customStart = null;
        group.customEnd = null;

        this.drawWaveform();
        this.updateGroupDisplay(groupIdx);
        this.updateResetButton(groupIdx);
    }

    getGroupColor(idx) {
        const colors = ['#22c55e', '#3b82f6', '#f59e0b', '#ec4899', '#8b5cf6', '#06b6d4', '#84cc16', '#f43f5e'];
        return colors[idx % colors.length];
    }

    getSpeakerColor(speaker) {
        const colors = ['#22c55e', '#3b82f6', '#f59e0b', '#ec4899', '#8b5cf6', '#06b6d4'];
        const idx = parseInt(speaker.replace('SPEAKER_', '')) || 0;
        return colors[idx % colors.length];
    }

    mergeWithNext(groupIdx) {
        if (groupIdx >= this.groups.length - 1) return;
        
        // Сохраняем транскрипции
        this.saveCurrentTranscriptsToMemory();
        
        const current = this.groups[groupIdx];
        const next = this.groups[groupIdx + 1];
        
        const customStart = current.customStart;
        const customEnd = next.customEnd;
        
        current.segments = [...current.segments, ...next.segments];
        current.transcript = [current.transcript, next.transcript].filter(t => t).join(' ');
        current.customStart = customStart;
        current.customEnd = customEnd;
        
        this.groups.splice(groupIdx + 1, 1);
        this.render();
        this.drawWaveform();
    }

    splitGroupDialog(groupIdx) {
        this.saveCurrentTranscriptsToMemory();
        
        const group = this.groups[groupIdx];
        if (group.segments.length <= 1) {
            showToast('Cannot split - only 1 segment', true);
            return;
        }
        
        // Показываем диалог выбора где разделить
        let options = 'Split after segment:\n\n';
        group.segments.forEach((seg, i) => {
            if (i < group.segments.length - 1) {
                options += `${i + 1}: ${this.formatTime(seg.start)} - ${this.formatTime(seg.end)} (${seg.speaker})\n`;
            }
        });
        
        const choice = prompt(options + '\nEnter number:');
        if (choice) {
            const idx = parseInt(choice);
            if (idx > 0 && idx < group.segments.length) {
                this.splitGroup(groupIdx, idx);
            }
    
        }
    }

    splitGroup(groupIdx, afterSegmentIdx) {
        const group = this.groups[groupIdx];
        
        const firstSegments = group.segments.slice(0, afterSegmentIdx);
        const secondSegments = group.segments.slice(afterSegmentIdx);
        
        const newGroups = [
            {
                segments: firstSegments,
                transcript: '',
                customStart: group.customStart,
                customEnd: null
            },
            {
                segments: secondSegments,
                transcript: group.transcript,
                customStart: null,
                customEnd: group.customEnd
            }
        ];
        
        this.groups.splice(groupIdx, 1, ...newGroups);
        this.render();
        this.drawWaveform();
    }

    playSegment(groupIdx) {
        const group = this.groups[groupIdx];
        if (!group || !this.audioElement) return;
        
        const start = this.getGroupStart(group);
        const end = this.getGroupEnd(group);
        
        this.audioElement.currentTime = start;
        this.audioElement.play();
        
        const stopHandler = () => {
            if (this.audioElement.currentTime >= end) {
                this.audioElement.pause();
                this.audioElement.removeEventListener('timeupdate', stopHandler);
            }
        };
        this.audioElement.addEventListener('timeupdate', stopHandler);
    }

    async exportSegments() {
        // Собираем данные групп с кастомными границами
        const groups = this.groups.map((group, idx) => {
            const textarea = document.getElementById(`group-transcript-${idx}`);
            return {
                start: this.getGroupStart(group),
                end: this.getGroupEnd(group),
                transcript: textarea ? textarea.value : group.transcript,
                speaker: group.segments[0]?.speaker || 'SPEAKER_00'
            };
        });

        if (groups.some(g => !g.transcript.trim())) {
            if (!confirm('Some groups have empty transcripts. Continue anyway?')) {
                return;
            }
        }

        const btn = document.querySelector('.segments-actions button:last-child');
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Exporting...';
        }

        try {
            const resp = await fetch(`/api/files/${this.fileId}/segments/export`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ groups })
            });
            const data = await resp.json();
            
            if (data.success) {
                showToast(`Created ${data.data.created} audio files`);
                // Перезагрузить список файлов
                if (typeof loadFiles === 'function') {
                    loadFiles();
                }
                closeModal();
            } else {
                showToast('Error: ' + data.error, true);
            }
        } catch (e) {
            showToast('Export error: ' + e.message, true);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.textContent = '✂️ Export & Split';
            }
        }
    }

    async trimToSelection() {
        if (this.groups.length === 0) {
            showToast('No segments to trim', true);
            return;
        }

        const start = this.getGroupStart(this.groups[0]);
        const end = this.getGroupEnd(this.groups[this.groups.length - 1]);

        if (!confirm(`Trim audio to ${this.formatTime(start)} - ${this.formatTime(end)}?\nDuration: ${(end - start).toFixed(3)}s\n\nThis will REPLACE the original file!`)) {
            return;
        }

        try {
            const resp = await fetch(`/api/files/${this.fileId}/trim`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ start, end })
            });
            const data = await resp.json();
            
            if (data.success) {
                showToast(`Trimmed! New duration: ${data.data.new_duration.toFixed(2)}s`);
                
                // Устанавливаем антикэш для этого файла
                if (typeof audioCacheBust !== 'undefined') {
                    audioCacheBust[this.fileId] = Date.now();
                }
                
                closeModal();
                if (typeof loadFiles === 'function') {
                    loadFiles();
                }
            } else {
                showToast('Error: ' + data.error, true);
            }
        } catch (e) {
            showToast('Trim error: ' + e.message, true);
        }
    }

    async saveTranscripts() {
        const allTranscripts = {};
        this.groups.forEach((group) => {
            const textarea = document.getElementById(`group-transcript-${this.groups.indexOf(group)}`);
            if (textarea) {
                group.transcript = textarea.value;
            }
            const text = group.transcript;
            group.segments.forEach((seg, i) => {
                allTranscripts[seg.id] = i === 0 ? text : '';
            });
        });

        try {
            const resp = await fetch(`/api/files/${this.fileId}/segments/transcripts`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ transcripts: allTranscripts })
            });
            const data = await resp.json();
            if (data.success) {
                showToast(`Saved ${this.groups.length} group transcripts`);
            }
        } catch (e) {
            showToast('Save error', true);
        }
    }

    async applyToOriginal() {
        if (!confirm('Apply group transcripts to original?')) return;

        await this.saveTranscripts();
        
        try {
            const resp = await fetch(`/api/files/${this.fileId}/segments/apply`, { method: 'POST' });
            const data = await resp.json();
            if (data.success) {
                showToast('Applied to original');
                if (typeof showDetail === 'function') {
                    showDetail(this.fileId);
                }
            } else {
                showToast('Error: ' + data.error, true);
            }
        } catch (e) {
            showToast('Apply error', true);
        }
    }

    formatTime(sec) {
        const m = Math.floor(sec / 60);
        const s = (sec % 60).toFixed(1);
        return `${m}:${s.padStart(4, '0')}`;
    }

    render() {
        const container = document.getElementById('segments-container');
        if (!container) return;

        this.audioElement = document.querySelector('#modal-content audio');

        if (!this.segments || this.segments.length === 0) {
            container.innerHTML = `
                <div class="segments-panel">
                    <div class="segments-empty">
                        <p>No segments found</p>
                        <button id="btn-diarize" class="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600" onclick="segmentManager.diarize()">
                            🎯 Split Audio
                        </button>
                    </div>
                </div>`;
            return;
        }

        let html = `
            <div class="segments-panel">
                <div class="segments-header">
                    <span class="font-bold text-gray-700">🎯 Audio Segments</span>
                    <button id="btn-diarize" class="bg-gray-500 text-white px-2 py-1 rounded text-xs hover:bg-gray-600 ml-2" onclick="segmentManager.diarize()">🔄 Re-split</button>
                    <span class="text-gray-500 text-sm ml-auto">${this.groups.length} groups, ${this.segments.length} segments</span>
                </div>
                
                <div class="waveform-container">
                    <canvas id="waveform-canvas" width="700" height="100"></canvas>
                    <div class="waveform-hint">Drag edges to adjust boundaries | Use ◀▶ for fine control</div>
                </div>
                
                <div class="groups-list">`;

        this.groups.forEach((group, groupIdx) => {
            const startTime = this.getGroupStart(group);
            const endTime = this.getGroupEnd(group);
            const duration = (endTime - startTime).toFixed(1);
            const color = this.getGroupColor(groupIdx);
            const speakers = [...new Set(group.segments.map(s => s.speaker))].join(', ');
            const hasOverlap = group.segments.some(s => s.has_overlap);
            const hasCustom = group.customStart !== null || group.customEnd !== null;

            html += `
                <div id="group-item-${groupIdx}" class="group-item" style="border-left: 4px solid ${color}">
                    <div class="group-header">
                        <button class="play-btn" onclick="segmentManager.playSegment(${groupIdx})" title="Play">▶</button>
                        <span id="group-time-${groupIdx}" class="group-time">${this.formatTime(startTime)} - ${this.formatTime(endTime)}</span>
                        <span id="group-dur-${groupIdx}" class="group-duration">(${duration}s)</span>
                        <span class="group-speakers">${speakers}</span>
                        ${hasOverlap ? '<span class="overlap-badge">OVERLAP</span>' : ''}
                        ${hasCustom ? `<span id="adjusted-badge-${groupIdx}" class="custom-badge">ADJUSTED</span>` : ''}
                        <span class="group-seg-count">${group.segments.length} seg</span>
                        
                        <div class="group-actions">
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'start', -0.5)" title="Start -0.5s">⏪</button>
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'start', -0.1)" title="Start -0.1s">◀</button>
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'start', 0.1)" title="Start +0.1s">▶</button>
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'start', 0.5)" title="Start +0.5s">⏩</button>
                            <span class="adj-sep">│</span>
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'end', -0.5)" title="End -0.5s">⏪</button>
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'end', -0.1)" title="End -0.1s">◀</button>
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'end', 0.1)" title="End +0.1s">▶</button>
                            <button class="adj-btn" onclick="segmentManager.adjustBoundary(${groupIdx}, 'end', 0.5)" title="End +0.5s">⏩</button>
                            <button id="reset-btn-${groupIdx}" class="reset-btn" onclick="segmentManager.resetBoundaries(${groupIdx})" title="Reset" style="display:${hasCustom ? 'inline-block' : 'none'}">↺</button>
                            ${group.segments.length > 1 ? 
                                `<button class="split-btn" onclick="segmentManager.splitGroupDialog(${groupIdx})" title="Split group">✂</button>` : ''}
                            ${groupIdx < this.groups.length - 1 ? 
                                `<button class="merge-btn" onclick="segmentManager.mergeWithNext(${groupIdx})" title="Merge with next">⊕</button>` : ''}
                        </div>
                    </div>
                    
                    <textarea id="group-transcript-${groupIdx}" 
                              class="group-transcript" 
                              placeholder="Enter transcript for this group...">${group.transcript}</textarea>
                </div>`;
        });

        html += `
                </div>
                
                <div class="segments-actions">
                    <button class="bg-green-500 text-white px-4 py-2 rounded hover:bg-green-600" onclick="segmentManager.saveTranscripts()">💾 Save</button>
                    <button class="bg-orange-500 text-white px-4 py-2 rounded hover:bg-orange-600" onclick="segmentManager.trimToSelection()">✂️ Trim to Selection</button>
                    <button class="bg-purple-500 text-white px-4 py-2 rounded hover:bg-purple-600" onclick="segmentManager.exportSegments()">✂️ Export & Split</button>
                </div>
            </div>`;

        container.innerHTML = html;
        
        setTimeout(() => {
            this.drawWaveform();
            this.setupCanvasEvents();
        }, 100);
    }

    showSplitOptions(groupIdx) {
        const group = this.groups[groupIdx];
        if (group.segments.length <= 1) return;
        
        const options = group.segments.slice(1).map((seg, i) => 
            `${i + 1}: after ${this.formatTime(group.segments[i].end)}`
        ).join('\n');
        
        const choice = prompt(`Split after which segment?\n${options}\n\nEnter number:`);
        if (choice) {
            const idx = parseInt(choice);
            if (idx > 0 && idx < group.segments.length) {
                this.splitGroup(groupIdx, idx);
            }
        }
    }

    


}

let segmentManager = null;

function initSegments(fileId) {
    segmentManager = new SegmentManager(fileId);
    segmentManager.loadSegments();
}