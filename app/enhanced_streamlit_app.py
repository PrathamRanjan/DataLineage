import streamlit as st
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import json
from datetime import datetime
import re
import sys
import io
import base64
from typing import Dict, List, Any, Optional

# Add tools directory to path for impact analysis
sys.path.append(str(Path(__file__).parent.parent / "tools"))
from impact_analysis import LineageAnalyzer

# Set page config
st.set_page_config(
    page_title="Enterprise Data Lineage Platform",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced styling
st.markdown("""
<style>
.metric-container {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 1rem;
    border-radius: 0.5rem;
    margin: 0.5rem 0;
}

.node-details {
    background: #f8f9fa;
    border: 1px solid #dee2e6;
    border-radius: 0.375rem;
    padding: 1rem;
    margin: 0.5rem 0;
}

.field-lineage {
    background: #e7f3ff;
    border-left: 4px solid #0066cc;
    padding: 1rem;
    margin: 0.5rem 0;
}

.transformation-box {
    background: #fff3cd;
    border: 1px solid #ffecb5;
    border-radius: 0.25rem;
    padding: 0.75rem;
    margin: 0.25rem 0;
    font-family: monospace;
}

.impact-analysis {
    background: #d1ecf1;
    border: 1px solid #bee5eb;
    border-radius: 0.25rem;
    padding: 1rem;
    margin: 0.5rem 0;
}

.export-section {
    background: #f8f9fa;
    border: 1px solid #dee2e6;
    border-radius: 0.375rem;
    padding: 1rem;
    margin: 1rem 0;
}

.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
}

.stTabs [data-baseweb="tab"] {
    padding-left: 12px;
    padding-right: 12px;
}
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_enhanced_lineage_data():
    """Load enhanced lineage data with impact analysis capabilities."""
    try:
        # Load the main events CSV
        events_path = Path("lineage/events/events.csv")
        if not events_path.exists():
            st.error("Lineage events file not found. Please run the ETL pipelines first.")
            return None, None, None
        
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
        
        # Initialize impact analyzer
        try:
            analyzer = LineageAnalyzer()
            return df, json_events, analyzer
        except Exception as e:
            st.warning(f"Impact analyzer not available: {e}")
            return df, json_events, None
        
    except Exception as e:
        st.error(f"Error loading lineage data: {e}")
        return None, None, None

@st.cache_data
def load_sample_data():
    """Load sample data for inspection."""
    sample_data = {}
    
    # Load processed data samples
    processed_dir = Path("data/processed")
    if processed_dir.exists():
        for csv_file in processed_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file)
                sample_data[csv_file.name] = df.head(10)  # First 10 rows
            except Exception as e:
                st.warning(f"Could not load sample data from {csv_file}: {e}")
    
    # Load raw data samples
    raw_dir = Path("data/raw")
    if raw_dir.exists():
        for csv_file in raw_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file)
                sample_data[csv_file.name] = df.head(10)  # First 10 rows
            except Exception as e:
                st.warning(f"Could not load sample data from {csv_file}: {e}")
    
    return sample_data

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

def create_enhanced_lineage_graph(df, enable_click_events=True):
    """Create an enhanced interactive network graph with click capabilities."""
    G = nx.DiGraph()
    node_info = {}  # Store detailed node information
    
    for _, row in df.iterrows():
        if row['status'] != 'COMPLETED':
            continue
            
        job_name = row['job_name']
        
        # Store job information
        node_info[job_name] = {
            'type': 'job',
            'namespace': row['job_namespace'],
            'duration_ms': row['duration_ms'],
            'event_time': row['event_time'],
            'status': row['status'],
            'row_counts': {
                'input': row['rowcount_in'],
                'output': row['rowcount_out']
            },
            'transformations': row.get('transform', ''),
            'column_mappings': parse_column_mappings(row.get('columns_map', ''))
        }
        
        # Add job node
        G.add_node(job_name, 
                  node_type='job',
                  **node_info[job_name])
        
        # Add input datasets
        if pd.notna(row['input_names']) and row['input_names']:
            for input_name in row['input_names'].split(','):
                input_name = input_name.strip()
                if input_name:
                    node_info[input_name] = {
                        'type': 'dataset',
                        'namespace': 'raw_data',
                        'row_count': row['rowcount_in'],
                        'data_type': 'input'
                    }
                    
                    G.add_node(input_name, 
                              node_type='dataset',
                              **node_info[input_name])
                    G.add_edge(input_name, job_name)
        
        # Add output datasets
        if pd.notna(row['output_names']) and row['output_names']:
            for output_name in row['output_names'].split(','):
                output_name = output_name.strip()
                if output_name:
                    node_info[output_name] = {
                        'type': 'dataset',
                        'namespace': 'processed_data',
                        'row_count': row['rowcount_out'],
                        'data_type': 'output'
                    }
                    
                    G.add_node(output_name, 
                              node_type='dataset',
                              **node_info[output_name])
                    G.add_edge(job_name, output_name)
    
    return G, node_info

def create_enhanced_plotly_graph(G, node_info):
    """Create an enhanced interactive Plotly graph with click handling."""
    if len(G.nodes()) == 0:
        return None
    
    # Calculate layout with better positioning
    pos = nx.spring_layout(G, k=3, iterations=100, seed=42)
    
    # Separate nodes by type
    job_nodes = [n for n in G.nodes() if G.nodes[n].get('node_type') == 'job']
    dataset_nodes = [n for n in G.nodes() if G.nodes[n].get('node_type') == 'dataset']
    
    # Prepare enhanced node traces
    fig = go.Figure()
    
    # Add edges with better styling
    edge_x = []
    edge_y = []
    edge_info = []
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        
        # Determine edge direction and type
        source_type = G.nodes[edge[0]].get('node_type')
        target_type = G.nodes[edge[1]].get('node_type')
        edge_info.append(f"{edge[0]} → {edge[1]} ({source_type} to {target_type})")
    
    # Add edges
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=2, color='rgba(136,136,136,0.6)'),
        hoverinfo='none',
        mode='lines',
        name='Data Flow',
        showlegend=False
    ))
    
    # Add job nodes with enhanced interactivity
    if job_nodes:
        job_x = [pos[node][0] for node in job_nodes]
        job_y = [pos[node][1] for node in job_nodes]
        
        job_hover_text = []
        job_custom_data = []
        
        for node in job_nodes:
            info = node_info.get(node, {})
            hover_text = (
                f"<b>Job: {node}</b><br>"
                f"Namespace: {info.get('namespace', 'N/A')}<br>"
                f"Duration: {info.get('duration_ms', 'N/A')}ms<br>"
                f"Input Rows: {info.get('row_counts', {}).get('input', 'N/A')}<br>"
                f"Output Rows: {info.get('row_counts', {}).get('output', 'N/A')}<br>"
                f"Click for details"
            )
            job_hover_text.append(hover_text)
            job_custom_data.append({'node_name': node, 'node_type': 'job'})
        
        fig.add_trace(go.Scatter(
            x=job_x, y=job_y,
            mode='markers+text',
            hovertemplate='%{hovertext}<extra></extra>',
            hovertext=job_hover_text,
            text=job_nodes,
            textposition="middle center",
            marker=dict(
                size=30,
                color='lightblue',
                symbol='square',
                line=dict(width=3, color='darkblue')
            ),
            name='ETL Jobs',
            customdata=job_custom_data
        ))
    
    # Add dataset nodes with enhanced interactivity
    if dataset_nodes:
        dataset_x = [pos[node][0] for node in dataset_nodes]
        dataset_y = [pos[node][1] for node in dataset_nodes]
        
        dataset_hover_text = []
        dataset_custom_data = []
        dataset_colors = []
        
        for node in dataset_nodes:
            info = node_info.get(node, {})
            data_type = info.get('data_type', 'unknown')
            namespace = info.get('namespace', 'unknown')
            
            hover_text = (
                f"<b>Dataset: {node}</b><br>"
                f"Namespace: {namespace}<br>"
                f"Type: {data_type}<br>"
                f"Row Count: {info.get('row_count', 'N/A')}<br>"
                f"Click for details"
            )
            dataset_hover_text.append(hover_text)
            dataset_custom_data.append({'node_name': node, 'node_type': 'dataset'})
            
            # Color coding by namespace
            if namespace == 'raw_data':
                dataset_colors.append('lightgreen')
            elif namespace == 'processed_data':
                dataset_colors.append('lightcoral')
            else:
                dataset_colors.append('lightgray')
        
        fig.add_trace(go.Scatter(
            x=dataset_x, y=dataset_y,
            mode='markers+text',
            hovertemplate='%{hovertext}<extra></extra>',
            hovertext=dataset_hover_text,
            text=dataset_nodes,
            textposition="middle center",
            marker=dict(
                size=25,
                color=dataset_colors,
                symbol='circle',
                line=dict(width=2, color='darkgreen')
            ),
            name='Datasets',
            customdata=dataset_custom_data
        ))
    
    # Enhanced layout
    fig.update_layout(
        title={
            'text': "Interactive Data Lineage Graph",
            'x': 0.5,
            'font': {'size': 24}
        },
        showlegend=True,
        hovermode='closest',
        margin=dict(b=40,l=20,r=20,t=60),
        annotations=[ 
            dict(
                text="🖱️ Click nodes for detailed inspection • Hover for quick info",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.5, y=-0.05,
                xanchor='center', yanchor='bottom',
                font=dict(color='gray', size=14)
            )
        ],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=700,
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    return fig

def display_node_details(node_name: str, node_type: str, node_info: Dict, sample_data: Dict, analyzer: Optional[LineageAnalyzer] = None):
    """Display detailed information about a selected node."""
    
    st.markdown(f"""
    <div class="node-details">
        <h3>🔍 {node_type.title()}: {node_name}</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs for different views
    if node_type == 'job':
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Job Details", "🔄 Transformations", "📈 Performance", "🎯 Impact Analysis"])
        
        with tab1:
            info = node_info.get(node_name, {})
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Duration", f"{info.get('duration_ms', 0)}ms")
                st.metric("Input Rows", f"{info.get('row_counts', {}).get('input', 0):,}")
                st.metric("Status", info.get('status', 'Unknown'))
            
            with col2:
                st.metric("Output Rows", f"{info.get('row_counts', {}).get('output', 0):,}")
                st.metric("Namespace", info.get('namespace', 'Unknown'))
                reduction = info.get('row_counts', {})
                if reduction.get('input', 0) > 0:
                    pct = ((reduction.get('input', 0) - reduction.get('output', 0)) / reduction.get('input', 0)) * 100
                    st.metric("Data Reduction", f"{pct:.1f}%")
        
        with tab2:
            st.subheader("🔄 Column Transformations")
            mappings = info.get('column_mappings', [])
            
            if mappings:
                for mapping in mappings:
                    st.markdown(f"""
                    <div class="transformation-box">
                        <strong>{mapping['target']}</strong> ← {' + '.join(mapping['sources'])}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("No column transformation details available")
            
            transform_steps = info.get('transformations', '').split(',')
            if transform_steps and transform_steps[0]:
                st.subheader("⚙️ Processing Steps")
                for i, step in enumerate(transform_steps, 1):
                    st.write(f"{i}. {step.strip()}")
        
        with tab3:
            st.subheader("📈 Performance Metrics")
            
            # Performance visualization
            perf_data = pd.DataFrame({
                'Metric': ['Input Rows', 'Output Rows', 'Duration (ms)'],
                'Value': [
                    info.get('row_counts', {}).get('input', 0),
                    info.get('row_counts', {}).get('output', 0),
                    info.get('duration_ms', 0)
                ]
            })
            
            fig = px.bar(perf_data, x='Metric', y='Value', 
                        title=f"Performance Metrics for {node_name}")
            st.plotly_chart(fig, use_container_width=True)
            
            # Efficiency metrics
            st.subheader("⚡ Efficiency Analysis")
            input_rows = info.get('row_counts', {}).get('input', 0)
            duration = info.get('duration_ms', 1)  # Avoid division by zero
            
            if input_rows > 0 and duration > 0:
                throughput = (input_rows / duration) * 1000  # rows per second
                st.metric("Throughput", f"{throughput:.0f} rows/sec")
            
        with tab4:
            if analyzer:
                st.subheader("🎯 Impact Analysis")
                
                # Show available fields for this job
                try:
                    # This would need to be enhanced based on your lineage structure
                    st.info("Impact analysis available via CLI: `python tools/impact_analysis.py trace <field_name>`")
                    
                    # Show example usage
                    st.code(f"""
# Trace upstream dependencies
python tools/impact_analysis.py trace net_amount

# Analyze downstream impact  
python tools/impact_analysis.py impact quantity
                    """)
                    
                except Exception as e:
                    st.error(f"Impact analysis error: {e}")
            else:
                st.info("Impact analyzer not available. Please check the installation.")
    
    elif node_type == 'dataset':
        tab1, tab2, tab3 = st.tabs(["📋 Schema & Data", "📊 Data Quality", "🔗 Lineage"])
        
        with tab1:
            info = node_info.get(node_name, {})
            
            # Dataset metadata
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Row Count", f"{info.get('row_count', 0):,}")
                st.metric("Namespace", info.get('namespace', 'Unknown'))
            with col2:
                st.metric("Data Type", info.get('data_type', 'Unknown'))
            
            # Sample data
            st.subheader("📄 Sample Data")
            if node_name in sample_data:
                df_sample = sample_data[node_name]
                st.dataframe(df_sample, use_container_width=True)
                
                # Schema information
                st.subheader("🗂️ Schema Information")
                schema_df = pd.DataFrame({
                    'Column': df_sample.columns,
                    'Data Type': [str(dtype) for dtype in df_sample.dtypes],
                    'Non-Null Count': [df_sample[col].count() for col in df_sample.columns],
                    'Null %': [f"{((len(df_sample) - df_sample[col].count()) / len(df_sample) * 100):.1f}%" 
                             for col in df_sample.columns]
                })
                st.dataframe(schema_df, use_container_width=True)
                
            else:
                st.info(f"Sample data not available for {node_name}")
        
        with tab2:
            if node_name in sample_data:
                df_sample = sample_data[node_name]
                
                # Data quality metrics
                st.subheader("📊 Data Quality Overview")
                
                quality_metrics = {
                    'Total Rows': len(df_sample),
                    'Total Columns': len(df_sample.columns),
                    'Numeric Columns': len(df_sample.select_dtypes(include=['number']).columns),
                    'Text Columns': len(df_sample.select_dtypes(include=['object']).columns),
                    'Date Columns': len(df_sample.select_dtypes(include=['datetime']).columns)
                }
                
                cols = st.columns(len(quality_metrics))
                for i, (metric, value) in enumerate(quality_metrics.items()):
                    cols[i].metric(metric, value)
                
                # Null value analysis
                st.subheader("🕳️ Missing Data Analysis")
                null_counts = df_sample.isnull().sum()
                null_pct = (null_counts / len(df_sample) * 100).round(2)
                
                null_df = pd.DataFrame({
                    'Column': null_counts.index,
                    'Null Count': null_counts.values,
                    'Null %': null_pct.values
                }).sort_values('Null Count', ascending=False)
                
                if null_df['Null Count'].sum() > 0:
                    fig = px.bar(null_df.head(10), x='Column', y='Null %', 
                                title="Missing Data by Column")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.success("✅ No missing data detected!")
                    
            else:
                st.info("Data quality analysis requires sample data")
        
        with tab3:
            if analyzer:
                st.subheader("🔗 Field-Level Lineage")
                
                if node_name in sample_data:
                    df_sample = sample_data[node_name]
                    
                    st.write("Select a field to trace its lineage:")
                    selected_field = st.selectbox(
                        "Field",
                        options=df_sample.columns.tolist(),
                        key=f"field_select_{node_name}"
                    )
                    
                    if selected_field and st.button(f"Trace {selected_field}", key=f"trace_{node_name}_{selected_field}"):
                        try:
                            # Trace field lineage
                            field_identifier = f"{node_name}.{selected_field}" if '.' not in selected_field else selected_field
                            result = analyzer.trace_field(field_identifier)
                            
                            st.markdown(f"""
                            <div class="field-lineage">
                                <h4>🔍 Upstream Lineage for {selected_field}</h4>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Display lineage tree
                            def display_lineage_tree(tree, level=0):
                                indent = "  " * level
                                transformation = tree.get('transformation', {})
                                
                                if level == 0:
                                    st.write(f"🎯 **{tree['name']}** ({tree['type']})")
                                else:
                                    transform_desc = transformation.get('description', 'Direct dependency')
                                    st.write(f"{indent}↳ **{tree['name']}** ← `{transform_desc}`")
                                
                                for upstream in tree.get('upstream', []):
                                    display_lineage_tree(upstream, level + 1)
                            
                            display_lineage_tree(result)
                            
                        except Exception as e:
                            st.error(f"Failed to trace field lineage: {e}")
                else:
                    st.info("Field lineage requires sample data")
            else:
                st.info("Lineage analysis requires the impact analyzer")

def export_report(df, node_info, format_type='pdf'):
    """Generate and provide download link for lineage reports."""
    
    if format_type == 'json':
        report_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_jobs': len([n for n in node_info.values() if n.get('type') == 'job']),
                'total_datasets': len([n for n in node_info.values() if n.get('type') == 'dataset'])
            },
            'jobs': {k: v for k, v in node_info.items() if v.get('type') == 'job'},
            'datasets': {k: v for k, v in node_info.items() if v.get('type') == 'dataset'},
            'lineage_events': df.to_dict('records')
        }
        
        json_str = json.dumps(report_data, indent=2, default=str)
        b64 = base64.b64encode(json_str.encode()).decode()
        href = f'<a href="data:file/json;base64,{b64}" download="lineage_report.json">📄 Download JSON Report</a>'
        return href
        
    elif format_type == 'csv':
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_str = csv_buffer.getvalue()
        b64 = base64.b64encode(csv_str.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="lineage_summary.csv">📊 Download CSV Report</a>'
        return href
    
    return None

def main():
    """Main Streamlit application with enhanced interactivity."""
    
    # Header with enhanced styling
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0;">
        <h1 style="color: #1f77b4; font-size: 3rem;">🔄 Enterprise Data Lineage Platform</h1>
        <p style="color: #666; font-size: 1.2rem;">Interactive visualization and impact analysis for data workflows</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Load enhanced data
    df, json_events, analyzer = load_enhanced_lineage_data()
    sample_data = load_sample_data()
    
    if df is None:
        return
    
    # Enhanced sidebar
    with st.sidebar:
        st.markdown("## 🎛️ Dashboard Controls")
        
        # Filter options
        completed_runs = df[df['status'] == 'COMPLETED']
        
        if len(completed_runs) == 0:
            st.error("No completed pipeline runs found")
            st.code("""
python run_pipeline.py --name etl_orders
python run_pipeline.py --name etl_customers_daily
            """)
            return
        
        # Job filter
        available_jobs = completed_runs['job_name'].unique()
        selected_jobs = st.multiselect(
            "📋 Select Jobs to Display",
            options=available_jobs,
            default=available_jobs,
            help="Filter the lineage graph by specific ETL jobs"
        )
        
        # Analysis tools
        st.markdown("## 🔧 Analysis Tools")
        
        if st.button("🔍 Run Lineage Validation", help="Check for circular dependencies and orphaned nodes"):
            if analyzer:
                with st.spinner("Running validation..."):
                    validation_result = analyzer.validate_lineage()
                    st.session_state['validation_result'] = validation_result
            else:
                st.error("Impact analyzer not available")
        
        # Export options
        st.markdown("## 📤 Export Options")
        export_format = st.selectbox("Report Format", ["JSON", "CSV"])
        
        if st.button("Generate Report"):
            G, node_info = create_enhanced_lineage_graph(completed_runs[completed_runs['job_name'].isin(selected_jobs)])
            download_link = export_report(completed_runs, node_info, export_format.lower())
            if download_link:
                st.markdown(download_link, unsafe_allow_html=True)
    
    # Main content with enhanced layout
    main_tab1, main_tab2, main_tab3, main_tab4, main_tab5 = st.tabs([
        "🌐 Interactive Graph", 
        "📊 Pipeline Analytics", 
        "🔍 Node Inspector", 
        "📈 Impact Analysis",
        "⚙️ System Health"
    ])
    
    with main_tab1:
        st.markdown("### 🌐 Interactive Data Lineage Graph")
        st.markdown("Click on any node to inspect its details in the **Node Inspector** tab")
        
        # Filter data
        filtered_df = completed_runs[completed_runs['job_name'].isin(selected_jobs)]
        
        if len(filtered_df) > 0:
            # Create and display enhanced graph
            G, node_info = create_enhanced_lineage_graph(filtered_df)
            fig = create_enhanced_plotly_graph(G, node_info)
            
            if fig:
                # Display graph with click handling
                selected_data = st.plotly_chart(fig, use_container_width=True, on_select="rerun")
                
                # Handle node selection
                if hasattr(selected_data, 'selection') and selected_data.selection:
                    # Store selected node in session state for cross-tab access
                    point_indices = selected_data.selection.get('point_indices', [])
                    if point_indices:
                        # This would need to be enhanced based on Plotly's selection API
                        st.info("Node selected! Switch to the 'Node Inspector' tab to see details.")
                
                # Graph statistics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Nodes", len(G.nodes()))
                with col2:
                    st.metric("Total Edges", len(G.edges()))
                with col3:
                    st.metric("ETL Jobs", len([n for n in G.nodes() if G.nodes[n].get('node_type') == 'job']))
                with col4:
                    st.metric("Datasets", len([n for n in G.nodes() if G.nodes[n].get('node_type') == 'dataset']))
                    
            else:
                st.info("No lineage data to display for selected filters.")
        else:
            st.info("Please select at least one job to display.")
    
    with main_tab2:
        st.markdown("### 📊 Pipeline Analytics Dashboard")
        
        # Enhanced metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_runs = len(df)
        successful_runs = len(df[df['status'] == 'COMPLETED'])
        failed_runs = len(df[df['status'] == 'FAILED'])
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        with col1:
            st.markdown(f"""
            <div class="metric-container">
                <h3>Success Rate</h3>
                <h2>{success_rate:.1f}%</h2>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-container">
                <h3>Total Runs</h3>
                <h2>{total_runs}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-container">
                <h3>Successful</h3>
                <h2>{successful_runs}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-container">
                <h3>Failed</h3>
                <h2>{failed_runs}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        # Performance analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("⏱️ Performance Trends")
            if len(completed_runs) > 0:
                perf_df = completed_runs.copy()
                perf_df['event_time'] = pd.to_datetime(perf_df['event_time'])
                
                fig = px.line(perf_df, x='event_time', y='duration_ms', 
                             color='job_name', title="Execution Time Over Time")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No completed runs to analyze")
        
        with col2:
            st.subheader("📊 Data Processing Volume")
            if len(completed_runs) > 0:
                vol_df = completed_runs.groupby('job_name').agg({
                    'rowcount_in': 'sum',
                    'rowcount_out': 'sum'
                }).reset_index()
                
                fig = px.bar(vol_df, x='job_name', y=['rowcount_in', 'rowcount_out'],
                            title="Input vs Output Row Counts by Job")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data to analyze")
    
    with main_tab3:
        st.markdown("### 🔍 Node Inspector")
        st.markdown("Select a node from the graph or choose one below for detailed inspection")
        
        # Manual node selection
        G, node_info = create_enhanced_lineage_graph(completed_runs)
        
        col1, col2 = st.columns(2)
        with col1:
            node_type = st.selectbox("Node Type", ["job", "dataset"])
        
        with col2:
            if node_type == "job":
                available_nodes = [name for name, info in node_info.items() if info.get('type') == 'job']
            else:
                available_nodes = [name for name, info in node_info.items() if info.get('type') == 'dataset']
            
            if available_nodes:
                selected_node = st.selectbox("Select Node", available_nodes)
                
                if selected_node:
                    display_node_details(selected_node, node_type, node_info, sample_data, analyzer)
            else:
                st.info(f"No {node_type} nodes found")
    
    with main_tab4:
        st.markdown("### 📈 Impact Analysis")
        
        if analyzer:
            st.markdown("""
            <div class="impact-analysis">
                <h4>🎯 Field-Level Impact Analysis</h4>
                <p>Trace upstream dependencies and analyze downstream impact of data changes</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Impact analysis interface
            analysis_type = st.radio("Analysis Type", ["Upstream Trace", "Downstream Impact"])
            
            # Get available fields that actually have lineage tracking
            # These are the fields that have transformation mappings
            tracked_fields = [
                'email_hash', 'is_high_value', 'clv_score', 'engagement_level',  # from customers_daily
                'net_amount', 'total_value', 'estimated_profit', 'order_size_category'  # from orders_clean  
            ]
            
            # Also add fields that can be used for impact analysis
            impact_fields = [
                'quantity', 'unit_price', 'discount',  # source fields for orders
                'email', 'total_spent', 'total_orders', 'total_quantity'  # source fields for customers
            ]
            
            available_fields = sorted(tracked_fields + impact_fields)
            
            if available_fields:
                selected_field = st.selectbox("Select Field", available_fields)
                
                if selected_field and st.button(f"Run {analysis_type}"):
                    try:
                        with st.spinner(f"Running {analysis_type.lower()}..."):
                            if analysis_type == "Upstream Trace":
                                result = analyzer.trace_field(selected_field)
                                st.success(f"✅ Upstream trace completed for {selected_field}")
                            else:
                                result = analyzer.analyze_impact(selected_field)
                                st.success(f"✅ Impact analysis completed for {selected_field}")
                            
                            # Display results in expandable format
                            with st.expander("📋 Detailed Results", expanded=True):
                                st.json(result)
                                
                    except Exception as e:
                        st.error(f"Analysis failed: {e}")
                        st.info("💡 Try using the CLI tool: `python tools/impact_analysis.py trace <field_name>`")
            else:
                st.info("No fields available for analysis. Please ensure sample data is loaded.")
        else:
            st.warning("Impact analyzer not available. Please check the installation.")
            
            # CLI usage guide
            st.markdown("""
            ### 🖥️ CLI Impact Analysis
            Use the command-line tool for advanced analysis:
            """)
            
            st.code("""
# Trace upstream lineage
python tools/impact_analysis.py trace net_amount
python tools/impact_analysis.py trace customers_daily.clv_score

# Analyze downstream impact  
python tools/impact_analysis.py impact quantity

# Export lineage graph
python tools/impact_analysis.py export --format json

# Validate lineage consistency
python tools/impact_analysis.py validate
            """)
    
    with main_tab5:
        st.markdown("### ⚙️ System Health & Validation")
        
        # Display validation results if available
        if 'validation_result' in st.session_state:
            result = st.session_state['validation_result']
            
            st.subheader("🔍 Lineage Validation Results")
            
            # Statistics
            stats = result.get('statistics', {})
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Nodes", stats.get('total_nodes', 0))
            with col2:
                st.metric("Total Edges", stats.get('total_edges', 0))
            with col3:
                st.metric("Max Depth", stats.get('max_depth', 0))
            with col4:
                datasets = stats.get('datasets', 0)
                fields = stats.get('fields', 0)
                st.metric("Data Assets", datasets + fields)
            
            # Issues
            issues_found = False
            
            if result.get('circular_dependencies'):
                st.error("⚠️ Circular Dependencies Detected")
                for dep in result['circular_dependencies']:
                    st.write(f"• {dep}")
                issues_found = True
            
            if result.get('orphaned_nodes'):
                st.warning("⚠️ Orphaned Nodes Found")
                for node in result['orphaned_nodes']:
                    st.write(f"• {node}")
                issues_found = True
            
            if not issues_found:
                st.success("✅ No lineage issues detected!")
                
        else:
            st.info("Click 'Run Lineage Validation' in the sidebar to check system health")
        
        # System information
        st.subheader("📊 System Information")
        
        system_info = {
            'Events Directory': str(Path("lineage/events").absolute()),
            'Total JSON Files': len(list(Path("lineage/events").glob("*.json"))),
            'CSV Events File': "✅ Present" if Path("lineage/events/events.csv").exists() else "❌ Missing",
            'Sample Data Files': len(sample_data),
            'Impact Analyzer': "✅ Available" if analyzer else "❌ Not Available"
        }
        
        for key, value in system_info.items():
            st.write(f"**{key}:** {value}")

if __name__ == "__main__":
    main()