import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
from matplotlib.animation import FuncAnimation
import pandas as pd
import os
import shutil
import time
from glob import glob

from streaming_constants import constants

serving = constants.object_constants['publication_distribution']['serving']
year_q_dist = constants.object_constants['publication_distribution']['year_q_dist']

def animate(i):
    if os.path.exists(os.path.join(serving, year_q_dist)):
        distribution = pd.read_csv(os.path.join(serving, year_q_dist))
        years = distribution['publication_year'].unique()
        axs.clear()
        for year in years:
            year_data = distribution[distribution['publication_year'] == year]
            x = year_data['quarter']
            y = year_data['publications']
            axs.plot(x, y, marker='o', label=year)
            axs.set_xlabel('quarters')
            axs.set_ylabel('publications')
            axs.legend(loc='best')
        plt.tight_layout()

figure, axs = plt.subplots(1, 1, figsize=(8,5))
# axs[1][1].remove()
animation = FuncAnimation(figure, animate, interval= 2000)
# from IPython.display import HTML
# HTML(animation.to_jshtml())
plt.suptitle('Distribution of Publications')
plt.tight_layout()
plt.show()