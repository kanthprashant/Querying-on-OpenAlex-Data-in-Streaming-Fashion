import matplotlib.pyplot as plt
plt.style.use('ggplot')
from matplotlib.animation import FuncAnimation
import pandas as pd
import os
import shutil
import time
from glob import glob
from streaming_constants import constants


serving = constants.object_constants['prominent_papers']['serving']
cncpts_file = constants.object_constants['prominent_papers']['cncpts_file']
cncpts = constants.object_constants['prominent_papers']['cncpts']

def update(ax, i):
    paper_count = pd.read_csv(os.path.join(serving, cncpts_file[i]))
    x = paper_count['id'].values.tolist()
    x = [iden.split('/')[-1] for iden in x]
    y = paper_count['cited_by_count']
    ax.clear()
    ax.set_xlabel('citations')
    ax.set_ylabel('works')
    ax.set_title(f'{cncpts[i]}')
    ax.barh(x, y)

def animate(i):
    if all([os.path.exists(os.path.join(serving, f)) for f in cncpts_file]):
        axises = axs.flatten()
        for i in range(len(cncpts)):
            update(axises[i], i)
        plt.tight_layout()

figure, axs = plt.subplots(2, 3, figsize=(15,10))
animation = FuncAnimation(figure, animate, interval= 2000)
# from IPython.display import HTML
# HTML(animation.to_jshtml())
plt.suptitle('Top 10 works')
plt.tight_layout()
plt.show()