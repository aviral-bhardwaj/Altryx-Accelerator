"""Original geometric product icon; deterministic text PDF of README.md."""
from pathlib import Path
import textwrap
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
assets = ROOT / 'marketplace/assets'
assets.mkdir(exist_ok=True)
icon = Image.new('RGB',(512,512),'#101c2d')
draw = ImageDraw.Draw(icon)
# Original connected flow nodes and an A-shaped bridge, no third-party marks.
for x,y in [(106,150),(106,362),(406,256)]:
    draw.rounded_rectangle((x-48,y-48,x+48,y+48),radius=20,fill='#26d8b2')
draw.line([(154,150),(255,150),(358,256)],fill='#e9f3fa',width=18)
draw.line([(154,362),(255,362),(358,256)],fill='#e9f3fa',width=18)
draw.ellipse((232,232,280,280),fill='#ffb74d')
icon.save(assets/'icon.png')

# Pillow's bundled default font is portable; the PDF contains the full README.
font = ImageFont.load_default(size=17)
lines=[]
for paragraph in (ROOT/'README.md').read_text().splitlines():
    lines.extend(textwrap.wrap(paragraph,width=95,replace_whitespace=False) or [''])
pages=[]
for start in range(0,len(lines),54):
    page=Image.new('RGB',(1200,1600),'white'); d=ImageDraw.Draw(page)
    d.text((65,45),'ALTRYX ACCELERATOR 1.1.0',font=font,fill='#14354a')
    for i,line in enumerate(lines[start:start+54]):
        d.text((65,100+i*25),line,font=font,fill='#202b35')
    d.text((65,1510),f'Vrahad Analytics | {start//54+1}',font=font,fill='#14354a')
    pages.append(page)
pages[0].save(assets/'README.pdf',save_all=True,append_images=pages[1:],resolution=144,
              title='Altryx Accelerator 1.1.0',author='Vrahad Analytics',
              creationDate='D:20261029000000Z',modDate='D:20261029000000Z')
