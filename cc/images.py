# https://pillow.readthedocs.io/en/3.1.x/reference/Image.html

import os, logging
from PIL import Image

def resize(input_file, output_file, size=None) -> None:
    try:
        img = Image.open(input_file)
        if size is None:
            # halves each dimension 
            size = int(img.size[0] / 2), int(img.size[1] / 2)
        img.thumbnail(size, Image.ANTIALIAS)
        img.save(output_file)  #, "PNG")
    except Exception as e:
        logging.error(f'images.resize {e}')
        
