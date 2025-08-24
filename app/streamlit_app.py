import streamlit as st
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import json
from datetime import datetime
import re

# Set page config
st.set_page_config(
    page_title="Data Lineage Visualizer",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_lineage_data():
    """Load lineage data from CSV and JSON files."""
    try:
        # Load the main events CSV
        events_path = Path("lineage/events/events.csv")
        if not events_path.exists():
            st.error("Lineage events file not found. Please run the ETL pipelines first.")
            return None, None
        
        df = pd.read_csv(events_path)
        
        # Load detailed JSON events
        json_files = list(Path("lineage/events").glob("*.json"))
        json_events = []
        
        for json_file in json_files:
            try:
                with open(json_file) as f:
                    event = json.load(f)
                    json_events.append(event)
            except Exception as e:
                st.warning(f"Could not load {json_file}: {e}")
        
        return df, json_events
    
    except Exception as e:
        st.error(f"Error loading lineage data: {e}")
        return None, None

def parse_column_mappings(columns_map_str):
    """Parse column mapping string into structured data."""
    if pd.isna(columns_map_str) or not columns_map_str:
        return []
    
    mappings = []
    for mapping in columns_map_str.split(';'):
        if ':' in mapping:
            target, sources = mapping.split(':', 1)
            source_cols = [s.strip() for s in sources.split('+')]
            mappings.append({
                'target': target.strip(),
                'sources': source_cols
            })
    return mappings

def create_lineage_graph(df):
    """Create a network graph from lineage data."""
    G = nx.DiGraph()
    
    for _, row in df.iterrows():
        if row['status'] != 'COMPLETED':
            continue
            
        job_name = row['job_name']
        
        # Add job node
        G.add_node(job_name, 
                  node_type='job',
                  namespace=row['job_namespace'],
                  run_count=1,
                  last_run=row['event_time'])
        
        # Add input datasets
        if pd.notna(row['input_names']) and row['input_names']:
            for input_name in row['input_names'].split(','):
                input_name = input_name.strip()
                if input_name:
                    G.add_node(input_name, 
                              node_type='dataset',
                              row_count=row['rowcount_in'])
                    G.add_edge(input_name, job_name)
        
        # Add output datasets
        if pd.notna(row['output_names']) and row['output_names']:
            for output_name in row['output_names'].split(','):
                output_name = output_name.strip()
                if output_name:
                    G.add_node(output_name, 
                              node_type='dataset',
                              row_count=row['rowcount_out'])
                    G.add_edge(job_name, output_name)
    
    return G

def create_plotly_graph(G):
    """Create an interactive Plotly graph visualization."""
    if len(G.nodes()) == 0:
        return None
    
    # Calculate layout
    pos = nx.spring_layout(G, k=3, iterations=50)
    
    # Prepare node traces
    job_nodes = [n for n in G.nodes() if G.nodes[n].get('node_type') == 'job']
    dataset_nodes = [n for n in G.nodes() if G.nodes[n].get('node_type') == 'dataset']
    
    # Job nodes
    job_x = [pos[node][0] for node in job_nodes]
    job_y = [pos[node][1] for node in job_nodes]
    job_text = [f"Job: {node}<br>Namespace: {G.nodes[node].get('namespace', 'N/A')}" for node in job_nodes]
    
    # Dataset nodes
    dataset_x = [pos[node][0] for node in dataset_nodes]
    dataset_y = [pos[node][1] for node in dataset_nodes]
    dataset_text = [f"Dataset: {node}<br>Rows: {G.nodes[node].get('row_count', 'N/A')}" for node in dataset_nodes]
    
    # Edge traces
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    # Create figure
    fig = go.Figure()
    
    # Add edges
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=2, color='#888'),
        hoverinfo='none',
        mode='lines',
        name='Data Flow'
    ))
    
    # Add job nodes
    fig.add_trace(go.Scatter(
        x=job_x, y=job_y,
        mode='markers+text',
        hoverinfo='text',
        hovertext=job_text,
        text=job_nodes,
        textposition="middle center",
        marker=dict(
            size=20,
            color='lightblue',
            symbol='square',
            line=dict(width=2, color='darkblue')
        ),
        name='ETL Jobs'
    ))
    
    # Add dataset nodes
    fig.add_trace(go.Scatter(
        x=dataset_x, y=dataset_y,
        mode='markers+text',
        hoverinfo='text',
        hovertext=dataset_text,
        text=dataset_nodes,
        textposition="middle center",
        marker=dict(
            size=15,
            color='lightgreen',
            symbol='circle',
            line=dict(width=2, color='darkgreen')
        ),
        name='Datasets'
    ))
    
    fig.update_layout(
        title="Data Lineage Graph",
        showlegend=True,
        hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        annotations=[ dict(
            text="Interactive Data Lineage Visualization",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.005, y=-0.002,
            xanchor='left', yanchor='bottom',
            font=dict(color='gray', size=12)
        )],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=600
    )
    
    return fig

def main():
    """Main Streamlit app."""
    st.title("🔄 Data Lineage Visualizer")
    st.markdown("Interactive visualization of ETL pipeline data flows and transformations")
    
    # Load data
    df, json_events = load_lineage_data()
    
    if df is None:
        return
    
    # Sidebar
    st.sidebar.header("📊 Dashboard Controls")
    
    # Filter options
    completed_runs = df[df['status'] == 'COMPLETED']
    
    if len(completed_runs) == 0:
        st.warning("No completed pipeline runs found. Please run the ETL pipelines first:")
        st.code("""
python run_pipeline.py --name etl_orders
python run_pipeline.py --name etl_customers_daily
        """)
        return
    
    # Job filter
    available_jobs = completed_runs['job_name'].unique()
    selected_jobs = st.sidebar.multiselect(
        "Select Jobs to Display",
        options=available_jobs,
        default=available_jobs
    )
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🌐 Lineage Graph")
        
        # Filter data
        filtered_df = completed_runs[completed_runs['job_name'].isin(selected_jobs)]
        
        if len(filtered_df) > 0:
            # Create and display graph
            G = create_lineage_graph(filtered_df)
            fig = create_plotly_graph(G)
            
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No lineage data to display for selected filters.")
        else:
            st.info("Please select at least one job to display.")
    
    with col2:
        st.subheader("📈 Pipeline Metrics")
        
        # Success rate
        total_runs = len(df)
        successful_runs = len(df[df['status'] == 'COMPLETED'])
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        st.metric("Success Rate", f"{success_rate:.1f}%")
        st.metric("Total Runs", total_runs)
        st.metric("Successful Runs", successful_runs)
        
        # Recent runs
        st.subheader("🕒 Recent Runs")
        recent_runs = df.sort_values('event_time', ascending=False).head(5)
        
        for _, run in recent_runs.iterrows():
            status_emoji = "✅" if run['status'] == 'COMPLETED' else "❌"
            st.write(f"{status_emoji} **{run['job_name']}**")
            st.write(f"   ⏱️ {run['duration_ms']}ms")
            st.write(f"   📅 {run['event_time'][:19]}")
            st.write("---")
    
    # Detailed view sections
    st.markdown("---")
    
    # Tab layout for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Run History", "🔍 Column Lineage", "📊 Data Quality", "🗂️ Raw Events"])
    
    with tab1:
        st.subheader("Pipeline Run History")
        display_df = df.copy()
        display_df['event_time'] = pd.to_datetime(display_df['event_time'])
        display_df = display_df.sort_values('event_time', ascending=False)
        
        st.dataframe(
            display_df[['event_time', 'job_name', 'status', 'duration_ms', 'rowcount_in', 'rowcount_out']],
            use_container_width=True
        )
    
    with tab2:
        st.subheader("Column-Level Lineage")
        
        selected_job = st.selectbox(
            "Select a job to view column lineage:",
            options=completed_runs['job_name'].unique()
        )
        
        if selected_job:
            job_runs = completed_runs[completed_runs['job_name'] == selected_job]
            latest_run = job_runs.iloc[-1]
            
            if pd.notna(latest_run['columns_map']) and latest_run['columns_map']:
                mappings = parse_column_mappings(latest_run['columns_map'])
                
                st.write(f"**Transformations in {selected_job}:**")
                for mapping in mappings:
                    st.write(f"• **{mapping['target']}** ← {' + '.join(mapping['sources'])}")
            else:
                st.info("No column lineage information available for this job.")
    
    with tab3:
        st.subheader("Data Quality Metrics")
        
        # Row count changes
        quality_df = completed_runs.copy()
        quality_df['data_reduction'] = ((quality_df['rowcount_in'] - quality_df['rowcount_out']) / 
                                       quality_df['rowcount_in'] * 100).round(1)
        
        fig_quality = px.bar(
            quality_df, 
            x='job_name', 
            y='data_reduction',
            title="Data Reduction by Job (%)",
            labels={'data_reduction': 'Reduction %', 'job_name': 'Job Name'}
        )
        st.plotly_chart(fig_quality, use_container_width=True)
        
        st.write("**Data Processing Summary:**")
        for _, row in quality_df.iterrows():
            st.write(f"• **{row['job_name']}**: {row['rowcount_in']} → {row['rowcount_out']} rows ({row['data_reduction']:+.1f}%)")
    
    with tab4:
        st.subheader("Raw Event Data")
        
        if json_events:
            selected_event = st.selectbox(
                "Select an event to view details:",
                options=range(len(json_events)),
                format_func=lambda x: f"Event {x+1}: {json_events[x].get('eventType', 'Unknown')}"
            )
            
            if selected_event is not None:
                st.json(json_events[selected_event])
        else:
            st.info("No JSON events found.")

if __name__ == "__main__":
    main()