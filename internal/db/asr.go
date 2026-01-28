package db

func (db *DB) UpdateASR(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_asr = ?, wer = ?, cer = ?, 
		    asr_status = 'processed', processed_at = NOW()
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

// UpdateASRNoLM сохраняет результат Kaldi без LM
func (db *DB) UpdateASRNoLM(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_asr_nolm = ?, wer_nolm = ?, cer_nolm = ?, 
		    asr_nolm_status = 'processed'
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

// UpdateASRNoLMError помечает файл как ошибочный для NoLM
func (db *DB) UpdateASRNoLMError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET asr_nolm_status = 'error' WHERE id = ?`, id)
	return err
}

// UpdateASRNoLMMetrics обновляет только WER/CER для NoLM
func (db *DB) UpdateASRNoLMMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer_nolm = ?, cer_nolm = ? WHERE id = ?`, wer, cer, id)
	return err
}

func (db *DB) UpdateASRMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer = ?, cer = ? WHERE id = ?`, wer, cer, id)
	return err
}
