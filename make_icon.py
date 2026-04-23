"""Generate Vectis.ico — dark navy background with a stylised V."""
from PIL import Image, ImageDraw, ImageFont
import os

SIZES = [16, 32,48, 64, 128, 256]
BG   = (26, 26, 46)       # #1A1A2E
BLUE = (43, 108, 176)     # #2B6CB0
LT   = (255, 255, 255)    # white

frames = []
for sz in SIZES:
    img  = Image.new("RGBA", (sz, sz), BG)
    draw = ImageDraw.Draw(img)

    # Rounded-rect card (slightly inset)
    pad = max(1, sz // 12)
    r   = max(2, sz // 8)
    draw.rounded_rectangle([pad, pad, sz - pad - 1, sz - pad - 1],
                            radius=r, fill=BLUE)

    # Draw a bold V using two thick lines
    m   = sz // 8          # margin inside card
    cx  = sz // 2
    top = pad + m
    bot = sz - pad - m
    lft = pad + m
    rgt = sz - pad - m

    lw = max(1, sz // 12)

    # Left arm: top-left → bottom-centre
    draw.line([(lft, top), (cx, bot)], fill=LT, width=lw)
    # Right arm: top-right → bottom-centre
    draw.line([(rgt, top), (cx, bot)], fill=LT, width=lw)

    frames.append(img)

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Vectis.ico")
frames[0].save(out, format="ICO", sizes=[(s, s) for s in SIZES],
               append_images=frames[1:])
print(f"Icon saved: {out}")
