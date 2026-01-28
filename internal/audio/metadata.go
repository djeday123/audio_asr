package audio

import (
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
)

type Metadata struct {
	DurationSec float64 `json:"duration_sec"`
	SampleRate  int     `json:"sample_rate"`
	Channels    int     `json:"channels"`
	BitDepth    int     `json:"bit_depth"`
	FileSize    int64   `json:"file_size"`
	Codec       string  `json:"codec"`
	Format      string  `json:"format"`
}

func GetMetadata(path string) (*Metadata, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		path)

	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var probe struct {
		Streams []struct {
			SampleRate    string `json:"sample_rate"`
			Channels      int    `json:"channels"`
			BitsPerSample int    `json:"bits_per_sample"`
			CodecName     string `json:"codec_name"`
		} `json:"streams"`
		Format struct {
			Duration   string `json:"duration"`
			FormatName string `json:"format_name"`
		} `json:"format"`
	}

	if err := json.Unmarshal(out, &probe); err != nil {
		return nil, err
	}

	m := &Metadata{FileSize: fi.Size()}

	if probe.Format.Duration != "" {
		m.DurationSec, _ = strconv.ParseFloat(probe.Format.Duration, 64)
	}
	m.Format = probe.Format.FormatName

	if len(probe.Streams) > 0 {
		s := probe.Streams[0]
		m.SampleRate, _ = strconv.Atoi(s.SampleRate)
		m.Channels = s.Channels
		m.BitDepth = s.BitsPerSample
		m.Codec = s.CodecName
	}

	return m, nil
}

func (m *Metadata) ToJSON() string {
	b, _ := json.Marshal(m)
	return string(b)
}
