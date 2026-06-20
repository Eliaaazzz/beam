"""Render a per-second terminal-screencast MP4 from REAL Dataflow window logs.

Reads window lines captured from the live per-second Dataflow job
(_df_persec_windows.txt), reveals them one per second over a dark terminal
canvas, and encodes to H.264 with ffmpeg. The clip ends mid-stream (no summary
card) so it reads as an infinite, still-running stream.
"""

import os
import re
import subprocess

from PIL import Image, ImageDraw, ImageFont

ROOT = r"C:\Users\Aufb\Desktop\beam"
SRC = os.path.join(ROOT, "_df_persec_windows.txt")
FRAMES = os.path.join(ROOT, "_dfvid_frames")
OUT = os.path.join(ROOT, "unbounded_source_dataflow_persecond.mp4")

W, H = 1280, 720
MARGIN = 38
BAR_H = 46
LINE_H = 30
TOP = BAR_H + 20
VISIBLE = (H - TOP - MARGIN) // LINE_H

BG = (13, 14, 18)
BAR = (32, 34, 44)
GREEN = (106, 196, 106)
CYAN = (96, 202, 220)
WHITE = (236, 238, 240)
GRAY = (198, 200, 206)
YELLOW = (242, 212, 96)
DIM = (122, 126, 138)
TITLE = (150, 154, 168)


def font(name, size):
  try:
    return ImageFont.truetype(f"C:/Windows/Fonts/{name}", size)
  except Exception:
    return ImageFont.load_default()


MONO = font("consola.ttf", 22)
BOLD = font("consolab.ttf", 22)
TITLEF = font("consolab.ttf", 20)
CHAR_W = MONO.getlength("M")

WIN_RE = re.compile(
    r"Window \[[\d-]+ (\d\d:\d\d:\d\d)[^,]*,\s*[\d-]* ?(\d\d:\d\d:\d\d)\)"
    r".*?Count=(\d+),\s*Min=(\d+),\s*Max=(\d+),\s*output_lag_sec=([-\d.]+)")


def load_windows(limit=58):
  out = []
  if not os.path.exists(SRC):
    return out
  with open(SRC, encoding="utf-8", errors="ignore") as fh:
    for line in fh:
      m = WIN_RE.search(line)
      if not m:
        continue
      s, e, c, mn, mx, lag = m.groups()
      out.append(
          "Window %s->%s   Count=%s   Min=%-8s Max=%-8s lag=%.1fs" %
          (s, e, c, mn, mx, float(lag)))
  return out[-limit:]


HEAD = [
    ("# Apache Beam UnboundedSource on Google Cloud Dataflow (streaming, Runner v2)", "com"),
    ("# job elia-persecond-east1   1-second fixed windows   rate = 25/sec", "com"),
    ("# A new window prints every second; Max climbs forever; the stream never ends.", "com"),
    ("# tailing the live worker log ...", "com"),
    ("", "blank"),
]


def draw_line(d, x, y, text, kind):
  if kind == "com":
    d.text((x, y), text, font=MONO, fill=GREEN)
    return
  d.text((x, y), text, font=MONO, fill=GRAY)
  for pat, col in ((r"Max=\S+", YELLOW), (r"Count=\S+", CYAN), (r"lag=\S+", DIM)):
    for m in re.finditer(pat, text):
      px = x + MONO.getlength(text[:m.start()])
      d.text((px, y), m.group(), font=MONO, fill=col)


def render(lines, n):
  img = Image.new("RGB", (W, H), BG)
  d = ImageDraw.Draw(img)
  d.rectangle([0, 0, W, BAR_H], fill=BAR)
  d.ellipse([18, 16, 32, 30], fill=(231, 95, 86))
  d.ellipse([42, 16, 56, 30], fill=(244, 191, 79))
  d.ellipse([66, 16, 80, 30], fill=(95, 200, 102))
  d.text((100, 13),
         "Google Cloud Dataflow  -  UnboundedSource per-second stream  (live)",
         font=TITLEF, fill=TITLE)
  shown = lines[:n + 1]
  window = shown[-VISIBLE:]
  y = TOP
  last = None
  for text, kind in window:
    if kind != "blank":
      draw_line(d, MARGIN, y, text, kind)
      last = (MARGIN + MONO.getlength(text), y)
    y += LINE_H
  if last is not None:
    cx, cy = last
    d.rectangle([cx + 2, cy + 3, cx + 2 + CHAR_W, cy + LINE_H - 6], fill=(150, 200, 150))
  return img


def main():
  wins = load_windows()
  if len(wins) < 5:
    raise SystemExit("Not enough window lines captured (%d). Check the job." % len(wins))
  content = list(HEAD) + [(w, "out") for w in wins]
  os.makedirs(FRAMES, exist_ok=True)
  for f in os.listdir(FRAMES):
    os.remove(os.path.join(FRAMES, f))
  concat = []
  for i in range(len(content)):
    p = os.path.join(FRAMES, f"f{i:04d}.png")
    render(content, i).save(p)
    kind = content[i][1]
    dur = {"com": 0.7, "blank": 0.3, "out": 1.0}[kind]
    if i == 0:
      dur += 0.7
    if i == len(content) - 1:
      dur += 2.5
    concat.append((p.replace("\\", "/"), dur))
  listfile = os.path.join(FRAMES, "concat.txt")
  with open(listfile, "w", encoding="utf-8") as fh:
    for p, dur in concat:
      fh.write("file '%s'\n" % p)
      fh.write("duration %.2f\n" % dur)
    fh.write("file '%s'\n" % concat[-1][0])
  total = sum(d for _, d in concat)
  cmd = [
      "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listfile,
      "-fps_mode", "cfr", "-r", "30", "-pix_fmt", "yuv420p",
      "-c:v", "libx264", "-preset", "medium", "-crf", "20",
      "-movflags", "+faststart", OUT,
  ]
  r = subprocess.run(cmd, capture_output=True, text=True)
  if r.returncode != 0:
    print("FFMPEG FAILED:\n", r.stderr[-1500:])
    raise SystemExit(1)
  print("OK windows=%d frames=%d approx_duration=%.1fs" % (len(wins), len(content), total))
  print("OUTPUT:", OUT)


if __name__ == "__main__":
  main()
