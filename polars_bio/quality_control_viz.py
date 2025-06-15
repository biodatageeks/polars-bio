import matplotlib.pyplot as plt
import numpy as np
import polars as pl

def plot_base_content(df, figsize=(10, 6), title='Base per pos quantity', 
                      colors=None, save_path=None, dpi=100):
    if colors is None:
        colors = {'a': 'blue', 'c': 'orange', 'g': 'red', 't': 'green', 'n': 'lightblue'}
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=figsize)
    
    # Get positions (assuming they are 0-based indices)
    positions = np.arange(len(df))
    
    # Plot each base content
    for base in ['a', 'c', 'g', 't', 'n']:
        column = f"{base}_count"
        if column in df.columns:
            ax.plot(positions, df[column], label=base, color=colors[base], linewidth=1.5)
    
    # Add labels and title
    ax.set_xlabel('Position in read (bp)')
    ax.set_ylabel('Base content')
    ax.set_title(title)
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Add legend
    ax.legend(title='base', loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Set y-axis to start at 0
    ax.set_ylim(bottom=0)
    
    # Tight layout
    plt.tight_layout()

    plt.close(fig)
    
    # Save if requested
    if save_path:
        plt.savefig(save_path, dpi=dpi, bbox_inches='tight')

    return fig