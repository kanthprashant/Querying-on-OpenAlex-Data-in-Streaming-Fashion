import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
from matplotlib.animation import FuncAnimation
import pandas as pd
import os
import shutil
import time
from glob import glob

from streaming_constants import constants

cncpts = constants.object_constants['growing_research']['cncpts']
all_cncpts_file = constants.object_constants['growing_research']['all_cncpts_file']
selected_cpts_file = constants.object_constants['growing_research']['selected_cpts_file']
serving = constants.object_constants['growing_research']['serving']

def update(ax, idx, sel_research_count):
    selected_area = sel_research_count[sel_research_count['concept'].str.upper() == cncpts[idx]]
    x = selected_area['publication_year']
    y = selected_area['publications']
    ax.plot(x, y, label=cncpts[idx])

def animate(i):
    axises = axs.flatten()
    
    if os.path.exists(os.path.join(serving, all_cncpts_file)):
        research_count = pd.read_csv(os.path.join(serving, all_cncpts_file))
        x = research_count['concept']
        y = research_count['publications']
        axises[0].clear()
        axises[0].set_xlabel('publications')
        axises[0].set_ylabel('concepts')
        axises[0].set_title('Top 10 Research Areas')
        axises[0].barh(x, y)
    
    if os.path.exists(os.path.join(serving, selected_cpts_file)):
        axises[1].clear()
        axises[1].set_title('Selected Research Areas')
        sel_research_count = pd.read_csv(os.path.join(serving, selected_cpts_file))
        for idx, cpt in enumerate(cncpts):
            update(axises[1], idx, sel_research_count)
        axises[1].legend(loc='best')
        axises[1].set_xlabel('year')
        axises[1].set_ylabel('publications')
    
    plt.tight_layout()

figure, axs = plt.subplots(1, 2, figsize=(15,5))
# axs[1][1].remove()
animation = FuncAnimation(figure, animate, interval= 2000)
# from IPython.display import HTML
# HTML(animation.to_jshtml())
plt.suptitle('Growing Research Areas')
plt.tight_layout()
plt.show()