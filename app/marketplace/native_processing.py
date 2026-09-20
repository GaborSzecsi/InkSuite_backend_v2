"""Bounded local processing shared by the standalone worker and tests."""

import json
import math
import subprocess
import warnings
from pathlib import Path
from PIL import Image, ImageOps
from app.marketing.media_prepare import binary


def image_variants(source, directory):
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(source) as probe:
            if (
                probe.format not in ("JPEG", "PNG", "WEBP")
                or probe.width * probe.height > 40_000_000
                or getattr(probe, "n_frames", 1) != 1
            ):
                raise ValueError("unsupported_image")
            probe.verify()
        with Image.open(source) as original:
            fixed = ImageOps.exif_transpose(original)
            # New pixel-only image strips EXIF, GPS, comments and other source metadata.
            mode = "RGBA" if "A" in fixed.getbands() else "RGB"
            clean = Image.new(mode, fixed.size)
            clean.paste(fixed.convert(mode))
            width, height = clean.size
            variants = []
            for name, size in [("small", 480), ("medium", 960), ("large", 1920)]:
                picture = clean.copy()
                picture.thumbnail((size, size), Image.Resampling.LANCZOS)
                path = Path(directory) / (name + ".webp")
                picture.save(path, "WEBP", quality=82, method=4)
                variants.append(
                    {
                        "name": name,
                        "path": path,
                        "content_type": "image/webp",
                        "width": picture.width,
                        "height": picture.height,
                    }
                )
            return {
                "width": width,
                "height": height,
                "mime_type": Image.MIME.get(original.format, "image/webp"),
                "duration": None,
            }, variants


def command(args, timeout=600):
    subprocess.run(
        args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=True,
    )


def video_variants(source, directory):
    result = subprocess.run(
        [
            binary("ffprobe"),
            "-v",
            "error",
            "-protocol_whitelist",
            "file,pipe",
            "-format_whitelist",
            "mov",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
            str(source),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    info = json.loads(result.stdout)
    streams = [
        s
        for s in info["streams"]
        if s.get("codec_type") == "video"
        and not s.get("disposition", {}).get("attached_pic")
    ]
    if len(streams) != 1:
        raise ValueError("unsupported_video")
    stream = streams[0]
    width = int(stream["width"])
    height = int(stream["height"])
    duration = float(info["format"]["duration"])
    if (
        not math.isfinite(duration)
        or not 0 < duration <= 180
        or min(width, height) < 2
        or width * height > 4096 * 2160
    ):
        raise ValueError("video_limits")
    variants = []
    base = [
        binary("ffmpeg"),
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-protocol_whitelist",
        "file,pipe",
        "-format_whitelist",
        "mov",
        "-i",
        str(source),
    ]
    # Bound both sides, preserve all artwork, rotate using ffmpeg's source metadata.
    for name, edge in [("720p", 1280)] + (
        [("1080p", 1920)]
        if max(width, height) > 1280 or min(width, height) > 720
        else []
    ):
        path = Path(directory) / (name + ".mp4")
        short = 720 if name == "720p" else 1080
        scale = f"scale=w='min(if(gte(iw,ih),{edge},{short}),iw)':h='min(if(gte(iw,ih),{short},{edge}),ih)':force_original_aspect_ratio=decrease:force_divisible_by=2,setsar=1"
        command(
            base
            + [
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-map_metadata",
                "-1",
                "-map_chapters",
                "-1",
                "-vf",
                scale,
                "-r",
                "30",
                "-c:v",
                "libx264",
                "-threads",
                "2",
                "-preset",
                "medium",
                "-crf",
                "24",
                "-maxrate",
                "4M",
                "-bufsize",
                "8M",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-ac",
                "2",
                "-movflags",
                "+faststart",
                "-t",
                "180",
                str(path),
            ]
        )
        variants.append({"name": name, "path": path, "content_type": "video/mp4"})
    poster = Path(directory) / "poster.jpg"
    command(
        base
        + [
            "-map",
            "0:v:0",
            "-map_metadata",
            "-1",
            "-frames:v",
            "1",
            "-vf",
            "scale=w='min(960,iw)':h='min(960,ih)':force_original_aspect_ratio=decrease",
            "-q:v",
            "3",
            str(poster),
        ],
        60,
    )
    variants.append({"name": "poster", "path": poster, "content_type": "image/jpeg"})
    return {
        "width": width,
        "height": height,
        "duration": duration,
        "mime_type": "video/mp4",
    }, variants
