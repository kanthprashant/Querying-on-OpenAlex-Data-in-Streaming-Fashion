import matplotlib.pyplot as plt
plt.style.use('seaborn')
from matplotlib.animation import FuncAnimation
import pandas as pd
import os
import shutil
import time
from glob import glob

from streaming_constants import constants

serving = constants.object_constants['prominent_authors']['serving']
headings = ['Citations', 'Publications']
files = [constants.object_constants['prominent_authors']['citations'], 
         constants.object_constants['prominent_authors']['publications']]

def update_publications(ax, i):
    author_rankings = pd.read_csv(os.path.join(serving, files[i]))
    x = author_rankings['author'].values.tolist()
    y = author_rankings['publications']
    ax.clear()
    ax.set_xlabel('publications')
    ax.set_ylabel('authors')
    ax.set_title(f'{headings[i]}')
    ax.barh(x, y)

def update_citations(ax, i):
    author_rankings = pd.read_csv(os.path.join(serving, files[i]))
    x = author_rankings['author'].values.tolist()
    y = author_rankings['citations']
    ax.clear()
    ax.set_xlabel('citations')
    ax.set_ylabel('authors')
    ax.set_title(f'{headings[i]}')
    ax.barh(x, y)

def animate(i):
    if all([os.path.exists(os.path.join(serving, f)) for f in files]):
        axises = axs.flatten()
        update_citations(axises[0], 0)
        update_publications(axises[1], 1)
        plt.tight_layout()

figure, axs = plt.subplots(1, 2, figsize=(10,5))
animation = FuncAnimation(figure, animate, interval= 2000)
# from IPython.display import HTML
# HTML(animation.to_jshtml())
plt.suptitle('Top 10 authors based on citations and publications')
plt.tight_layout()
plt.show()
