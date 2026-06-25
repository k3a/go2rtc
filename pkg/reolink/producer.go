package reolink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/baichuan"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/h264/annexb"
	"github.com/AlexxIT/go2rtc/pkg/h265"
	"github.com/pion/rtp"
)

func (c *Client) GetMedias() []*core.Media {
	return c.medias
}

func (c *Client) Probe() error {
	c.logDebug("probing stream")

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	reader, err := c.bc.StartPreview(ctx, c.channel, c.stream)
	if err != nil {
		return fmt.Errorf("reolink: start preview for probe failed: %w", err)
	}
	c.reader = reader

	var vcodec, acodec *core.Codec
	var iframeTries int
	// Use a 10s timeout instead of core.ProbeTimeout (5s) because long-GOP cameras (e.g. Trackmix)
	// have 2-4s keyframe intervals, and any packet drop or missing VPS/SPS can require waiting
	// for a second keyframe, which would exceed a 5s timeout and cause random loading failures.
	timeout := time.After(10 * time.Second)

ProbeLoop:
	for (c.videoEnabled && vcodec == nil) || (c.audioEnabled && acodec == nil) {
		select {
		case <-timeout:
			if c.videoEnabled && vcodec == nil {
				return fmt.Errorf("reolink: probe timeout waiting for video iframe")
			}
			c.logDebug("probe timeout, proceeding with available codecs")
			break ProbeLoop
		case packet, ok := <-reader.Packets:
			if !ok {
				return c.bc.Err()
			}

			if packet.Kind == baichuan.MediaPacketIFrame && vcodec == nil {
				c.logDebug("probe got iframe codec=%s len=%d tries=%d", packet.Codec, len(packet.Data), iframeTries)
				saved := packet
				c.probeIFrame = &saved
				iframeTries++
				if packet.Codec == "H265" {
					nalus := splitAnnexB(packet.Data)
					nalus = filterH265DecodableNALs(nalus)
					nalus = reorderH265NALsForAccessUnit(nalus)

					var b []byte
					for _, n := range nalus {
						b = append(b, 0, 0, 0, 1)
						b = append(b, n...)
					}

					buf := annexb.EncodeToAVCC(b)
					if len(buf) >= 5 && h265.NALUType(buf) == h265.NALUTypeVPS {
						vcodec = h265.AVCCToCodec(buf)
						c.logDebug("probe H265 fmtp=%s", vcodec.FmtpLine)
					} else if iframeTries < 3 {
						c.logDebug("probe H265 iframe missing VPS, waiting for next one")
						c.probeIFrame = nil
					} else {
						c.logDebug("probe H265 iframe missing VPS, using bare codec")
						vcodec = &core.Codec{Name: core.CodecH265, ClockRate: 90000, PayloadType: core.PayloadTypeRAW}
					}
				} else {
					buf := annexb.EncodeToAVCC(packet.Data)
					if len(buf) >= 5 && h264.NALUType(buf) == h264.NALUTypeSPS {
						vcodec = h264.AVCCToCodec(buf)
						c.logDebug("probe H264 fmtp=%s", vcodec.FmtpLine)
					} else if iframeTries < 3 {
						c.logDebug("probe H264 iframe missing SPS, waiting for next one")
						c.probeIFrame = nil
					} else {
						c.logDebug("probe H264 iframe missing SPS, using bare codec")
						vcodec = &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: core.PayloadTypeRAW}
					}
				}
			} else if (packet.Kind == baichuan.MediaPacketAAC || packet.Kind == baichuan.MediaPacketADPCM) && acodec == nil {
				if packet.Kind == baichuan.MediaPacketAAC {
					if aac.IsADTS(packet.Data) {
						acodec = aac.ADTSToCodec(packet.Data)
						c.logDebug("probe got AAC (ADTS) rate=%d ch=%d fmtp=%s", acodec.ClockRate, acodec.Channels, acodec.FmtpLine)
					} else {
						config := aac.EncodeConfig(aac.TypeAACLC, 16000, 1, false)
						acodec = aac.ConfigToCodec(config)
						c.logDebug("probe got AAC (raw) rate=%d", acodec.ClockRate)
					}
				} else {
					acodec = &core.Codec{
						Name:      core.CodecPCMA,
						ClockRate: 8000,
						Channels:  1,
					}
					c.logDebug("probe got ADPCM -> PCMA rate=8000 ch=1")
				}
			}
		}
	}
	c.medias = []*core.Media{}

	if c.videoEnabled && vcodec != nil {
		c.medias = append(c.medias, &core.Media{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{vcodec},
		})
	}

	if c.audioEnabled && acodec != nil {
		if acodec.Name == core.CodecAAC {
			acodec.PayloadType = core.PayloadTypeRAW
		}
		c.medias = append(c.medias, &core.Media{
			Kind:      core.KindAudio,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{acodec},
		})
	}

	if c.stream != baichuan.StreamMain {
		c.medias = append(c.medias, &core.Media{
			Kind:      core.KindAudio,
			Direction: core.DirectionSendonly,
			Codecs: []*core.Codec{
				{Name: core.CodecPCMA, ClockRate: 8000},
				{Name: core.CodecPCMU, ClockRate: 8000},
				{Name: core.CodecPCML, ClockRate: 16000},
				{Name: core.CodecPCML, ClockRate: 8000},
			},
		})
	}

	vcodecName := "disabled"
	if vcodec != nil {
		vcodecName = vcodec.Name
	}
	c.logDebug("probe complete, video=%s audio=%v medias=%d", vcodecName, acodec != nil, len(c.medias))
	return nil
}

func (c *Client) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	for _, track := range c.receivers {
		if track.Codec == codec {
			return track, nil
		}
	}

	track := core.NewReceiver(media, codec)
	c.receivers = append(c.receivers, track)

	return track, nil
}

func (c *Client) Start() error {
	c.logDebug("Start() called, receivers=%d reader=%v", len(c.receivers), c.reader != nil)

	if len(c.receivers) == 0 {
		c.logDebug("no receivers, backchannel-only stream started")
		ch := make(chan struct{})
		c.cancel = func() {
			close(ch)
		}
		<-ch
		return nil
	}

	if c.reader == nil {
		ctx, cancel := context.WithCancel(context.Background())
		c.cancel = cancel

		reader, err := c.bc.StartPreview(ctx, c.channel, c.stream)
		if err != nil {
			return err
		}
		c.reader = reader
	}

	// Set up the wall-clock pacers.
	if !c.started.Swap(true) {
		pacerCtx, pacerCancel := context.WithCancel(context.Background())
		c.pacerCancel = pacerCancel

		c.videoPace.reset()
		c.videoPacer = &mediaPacer{
			ch:             make(chan pacedFrame, 400),
			maxLead:        3000 * time.Millisecond,
			initialLatency: 1500 * time.Millisecond,
			snapOnPast:     false,
			logf:           c.logWarn,
		}
		c.pacerWg.Add(1)
		go func() {
			defer c.pacerWg.Done()
			c.videoPacer.run(pacerCtx)
		}()

		c.audioPacer = &mediaPacer{
			ch:             make(chan pacedFrame, 200),
			maxLead:        2000 * time.Millisecond,
			initialLatency: 1500 * time.Millisecond,
			snapOnPast:     true,
			logf:           c.logWarn,
		}
		c.pacerWg.Add(1)
		go func() {
			defer c.pacerWg.Done()
			c.audioPacer.run(pacerCtx)
		}()
	}

	var videoCount, audioCount int

	// replay the I-Frame that was consumed during Probe
	if c.probeIFrame != nil {
		c.processPacket(*c.probeIFrame, &videoCount, &audioCount)
		c.probeIFrame = nil
	}

	for {
		packet, ok := <-c.reader.Packets
		if !ok {
			err := c.bc.Err()
			c.logWarn("camera disconnected: %v, attempting to reconnect", err)
			return err
		}

		c.processPacket(packet, &videoCount, &audioCount)
	}
}

func (c *Client) processPacket(packet baichuan.MediaPacket, videoCount, audioCount *int) {
	c.recv += len(packet.Data)

	switch packet.Kind {
	case baichuan.MediaPacketIFrame, baichuan.MediaPacketPFrame:
		if !c.videoEnabled {
			return
		}
		if !packet.HasTimestamp {
			return
		}

		if packet.Codec == "H265" {
			// NALU reordering...
			nalus := splitAnnexB(packet.Data)
			nalus = filterH265DecodableNALs(nalus)
			nalus = reorderH265NALsForAccessUnit(nalus)

			if len(nalus) == 0 {
				return
			}

			var buf []byte
			for _, n := range nalus {
				buf = append(buf, 0, 0, 0, 1)
				buf = append(buf, n...)
			}
			packet.Data = buf
		}

		packet.Data = annexb.EncodeToAVCC(packet.Data)

		continuousUS := c.videoTimestamps.unwrap(packet.TimestampMicrosecs)

		clockRate := 90000
		timestamp := rtpTimestampForClock(continuousUS, clockRate)

		pkt := &core.Packet{
			Header: rtp.Header{
				Marker:    true,
				Timestamp: timestamp,
			},
			Payload: packet.Data,
		}

		videoCodec := packet.Codec
		write := func(p *core.Packet) {
			for _, receiver := range c.receivers {
				if receiver.Codec.Name == core.CodecH264 || receiver.Codec.Name == core.CodecH265 {
					if receiver.Codec.Name == core.CodecH264 && videoCodec != "H264" {
						continue
					}
					if receiver.Codec.Name == core.CodecH265 && videoCodec != "H265" {
						continue
					}

					clone := *p
					receiver.WriteRTP(&clone)
				}
			}
		}

		dur := c.videoPace.durationForFrame(continuousUS)
		if c.videoPacer != nil {
			c.videoPacer.enqueue(pacedFrame{pkts: []*core.Packet{pkt}, write: write, duration: dur})
		} else {
			write(pkt)
		}
		*videoCount++

		if *videoCount <= 3 {
			c.logDebug("video pkt #%d codec=%s kind=%d len=%d ts=%d",
				*videoCount, packet.Codec, packet.Kind, len(pkt.Payload), pkt.Timestamp)
		}
	case baichuan.MediaPacketAAC:
		if !c.audioEnabled {
			return
		}

		const samplesPerAccessUnit = 1024
		var aacSampleRates = []uint32{
			96000,
			88200,
			64000,
			48000,
			44100,
			32000,
			24000,
			22050,
			16000,
			12000,
			11025,
			8000,
			7350,
		}

		var sampleRate uint32 = 16000

		var pkts []*core.Packet
		payload := packet.Data
		for len(payload) > 0 {
			// Parse ADTS access units
			if !aac.IsADTS(payload) {
				break // reached padding or invalid data
			}

			headerLen := aac.ADTSHeaderLen(payload)
			// Frame length is 13 bits starting at byte 3, bit 13.
			frameLen := (int(payload[3]&3) << 11) | (int(payload[4]) << 3) | ((int(payload[5]) >> 5) & 7)

			if frameLen < headerLen || frameLen > len(payload) {
				break // invalid frame length
			}

			sampleRateIndex := (payload[2] >> 2) & 0x0F
			if sampleRateIndex <= 12 {
				sampleRate = aacSampleRates[sampleRateIndex]
			}

			rawAAC := payload[headerLen:frameLen]

			pkt := &core.Packet{
				Header: rtp.Header{
					Version:   aac.RTPPacketVersionAAC,
					Marker:    true,
					Timestamp: uint32(len(pkts) * samplesPerAccessUnit),
				},
				Payload: rawAAC,
			}
			pkts = append(pkts, pkt)
			payload = payload[frameLen:]
		}

		if len(pkts) == 0 {
			return
		}

		hasExpectedTS := packet.HasTimestamp
		expectedTS := uint32(0)
		if hasExpectedTS {
			timestampMicroseconds := c.audioTimestamps.unwrap(packet.TimestampMicrosecs)
			expectedTS = rtpTimestampForClock(timestampMicroseconds, int(sampleRate))
		}

		// Initialize audio
		if *audioCount == 0 {
			c.nextAudioTS = 0
			if hasExpectedTS {
				c.nextAudioTS = expectedTS
			}
		}

		samples := len(pkts) * samplesPerAccessUnit

		baseTimestamp := c.nextAudioTS
		if hasExpectedTS {
			baseTimestamp = expectedTS
		}
		baseTimestamp = c.audioTimestampGuard.applyBaseToPackets(pkts, baseTimestamp, uint32(samples))
		for _, pkt := range pkts {
			pkt.Timestamp += baseTimestamp
		}
		c.nextAudioTS = baseTimestamp + uint32(samples)

		aacWrite := func(p *core.Packet) {
			for _, receiver := range c.receivers {
				if receiver.Codec.Name == core.CodecAAC {
					clone := *p
					receiver.WriteRTP(&clone)
				}
			}
		}

		if c.audioPacer != nil {
			paceDur := time.Microsecond * time.Duration(int64(samples)*1_000_000/int64(sampleRate))
			c.audioPacer.enqueue(pacedFrame{pkts: pkts, write: aacWrite, duration: paceDur})
		} else {
			for _, pkt := range pkts {
				aacWrite(pkt)
			}
		}

		*audioCount += len(pkts)
		if *audioCount <= 3 && len(pkts) > 0 {
			c.logDebug("audio pkt #%d len=%d ts=%d",
				*audioCount, len(pkts[0].Payload), pkts[0].Timestamp)
		}
	case baichuan.MediaPacketADPCM:
		if !c.audioEnabled {
			return
		}
		if c.adpcmDecoder == nil {
			c.adpcmDecoder = &baichuan.ADPCMDecoder{}
		}

		pcm := c.adpcmDecoder.Decode(packet.Data)
		pcma := baichuan.EncodePCMA(pcm)

		if len(pcma) == 0 {
			return
		}

		const sampleRate uint32 = 8000 // Reolink usually sends ADPCM at 8kHz
		const sampleSize = 1           // 8 bit mono - one byte

		hasExpectedTS := packet.HasTimestamp
		expectedTS := uint32(0)
		if hasExpectedTS {
			timestampMicroseconds := c.audioTimestamps.unwrap(packet.TimestampMicrosecs)
			expectedTS = rtpTimestampForClock(timestampMicroseconds, int(sampleRate))
		}

		// Initialize audio
		if *audioCount == 0 {
			c.nextAudioTS = 0
			if hasExpectedTS {
				c.nextAudioTS = expectedTS
			}
		}

		var pkts []*core.Packet
		payload := pcma
		timestamp := uint32(0)
		for len(payload) > 0 {
			chunkSize := 1460 // 1500 (UDP MTU) - 20 (IP header) - 8 (UDP header) - 12 (RTP header)
			if len(payload) < chunkSize {
				chunkSize = len(payload)
			}
			chunk := payload[:chunkSize]
			payload = payload[chunkSize:]

			pkt := &core.Packet{
				Header: rtp.Header{
					Marker:    true,
					Timestamp: timestamp,
				},
				Payload: chunk,
			}
			pkts = append(pkts, pkt)
			timestamp += uint32(chunkSize / sampleSize)
		}

		if len(pkts) == 0 {
			return
		}

		duration := uint32(len(pcm)) //#nosec G115
		baseTimestamp := c.nextAudioTS
		if hasExpectedTS {
			baseTimestamp = expectedTS
		}
		baseTimestamp = c.audioTimestampGuard.applyBaseToPackets(pkts, baseTimestamp, duration)
		for _, pkt := range pkts {
			pkt.Timestamp += baseTimestamp
		}
		c.nextAudioTS = baseTimestamp + duration

		pcmaWrite := func(p *core.Packet) {
			for _, receiver := range c.receivers {
				if receiver.Codec.Name == core.CodecPCMA {
					clone := *p
					receiver.WriteRTP(&clone)
				}
			}
		}

		if c.audioPacer != nil {
			paceDur := time.Microsecond * time.Duration(int64(len(pcm))*1_000_000/int64(sampleRate))
			c.audioPacer.enqueue(pacedFrame{pkts: pkts, write: pcmaWrite, duration: paceDur})
		} else {
			for _, pkt := range pkts {
				pcmaWrite(pkt)
			}
		}

		*audioCount += len(pkts)
		if *audioCount <= 3 && len(pkts) > 0 {
			c.logDebug("audio pkt #%d len=%d ts=%d",
				*audioCount, len(pkts[0].Payload), pkts[0].Timestamp)
		}
	}
}

func (c *Client) Stop() error {
	for _, receiver := range c.receivers {
		receiver.Close()
	}
	if c.sender != nil {
		c.sender.Close()
	}
	return c.Close()
}

func (c *Client) MarshalJSON() ([]byte, error) {
	info := &core.Connection{
		ID:         core.ID(c),
		FormatName: "reolink",
		Protocol:   "baichuan",
		Medias:     c.medias,
		Recv:       c.recv,
		Receivers:  c.receivers,
		Send:       c.send,
	}
	if c.sender != nil {
		info.Senders = []*core.Sender{c.sender}
	}
	return json.Marshal(info)
}

type timestampUnwrapper struct {
	highest uint64
	offset  uint64
	baseSet bool
	// nowUnixMicro is optional; when nil, time.Now().UnixMicro is used (first sample anchors to wall clock).
	nowUnixMicro func() int64
}

func (u *timestampUnwrapper) unwrap(ts32 uint32) uint64 {
	if !u.baseSet {
		nowFn := func() int64 { return time.Now().UnixMicro() }
		if u.nowUnixMicro != nil {
			nowFn = u.nowUnixMicro
		}
		micros := nowFn()
		if micros < 0 {
			micros = 0
		}
		systemMicro := uint64(micros)
		u.offset = systemMicro - uint64(ts32)
		u.highest = uint64(ts32)
		u.baseSet = true
		return systemMicro
	}

	continuous := unwrapTimestamp(ts32, u.highest)
	if continuous > u.highest {
		u.highest = continuous
	}
	return continuous + u.offset
}

func unwrapTimestamp(ts32 uint32, highest64 uint64) uint64 {
	if highest64 == 0 {
		return uint64(ts32)
	}

	high32 := highest64 >> 32
	cand1 := (high32 << 32) | uint64(ts32)

	cand2 := cand1
	if cand1 >= 0x100000000 {
		cand2 = cand1 - 0x100000000
	}

	cand3 := cand1 + 0x100000000

	absDiff := func(a, b uint64) uint64 {
		if a > b {
			return a - b
		}
		return b - a
	}

	bestCand := cand1
	bestDiff := absDiff(cand1, highest64)

	if diff2 := absDiff(cand2, highest64); diff2 < bestDiff {
		bestCand = cand2
		bestDiff = diff2
	}
	if diff3 := absDiff(cand3, highest64); diff3 < bestDiff {
		bestCand = cand3
	}

	return bestCand
}

type rtpTimestampGuard struct {
	offset uint32
	last   uint32
	set    bool
}

func (g *rtpTimestampGuard) next(ts uint32) uint32 {
	if !g.set {
		g.last = ts
		g.set = true
		return ts
	}
	adjusted := ts + g.offset
	if ts == g.last {
		g.offset = g.last + 1 - ts
		adjusted = g.last + 1
	} else if !rtpTimestampAfter(adjusted, g.last) {
		jumpBackward := uint32(int32(g.last - adjusted))
		if jumpBackward > 90000 {
			g.offset = g.last + 1 - ts
			adjusted = ts + g.offset
		} else {
			adjusted = g.last + 1
		}
	}
	g.last = adjusted
	return adjusted
}

func (g *rtpTimestampGuard) applyBaseToPackets(pkts []*rtp.Packet, base uint32, duration uint32) uint32 {
	if len(pkts) == 0 {
		return base
	}

	sum := base + pkts[0].Timestamp //#nosec G115
	first := sum + g.offset
	if g.set && sum == g.last {
		g.offset = 0
		first = sum
	}
	if g.set && rtpTimestampBefore(first, g.last) {
		jumpBackward := uint32(int32(g.last - first))
		if jumpBackward > 90000 {
			g.offset = g.last - sum
			first = sum + g.offset
		} else {
			first = g.last
		}
	}

	adjusted := first
	if duration == 0 {
		g.last = adjusted
	} else {
		g.last = adjusted + duration
	}
	g.set = true
	return adjusted - pkts[0].Timestamp
}

func rtpTimestampAfter(ts uint32, prev uint32) bool {
	return int32(ts-prev) > 0 //#nosec G115
}

func rtpTimestampBefore(ts uint32, prev uint32) bool {
	return int32(ts-prev) < 0 //#nosec G115
}

func rtpTimestampForClock(microseconds uint64, clockRate int) uint32 {
	seconds := microseconds / 1_000_000
	rem := microseconds % 1_000_000
	return uint32(seconds*uint64(clockRate) + (rem*uint64(clockRate))/1_000_000) //#nosec G115
}
