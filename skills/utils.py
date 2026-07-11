"""
Skills Utilities for Loops Data Ingestion Project

This module provides utility functions for working with skills in the context
of Nanobot agents and the run_demo.py workflow.

Usage:
    from skills.utils import load_skills_for_agent, get_agent_context
    
    # Load skills for a specific agent stage
    context = load_skills_for_agent(stage=1)  # Investigation
    
    # Get complete agent context
    context = get_agent_context(stage=2)  # Pipeline Builder
"""

from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import os
import sys


# Project configuration
PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")


# Stage definitions with their skill files and agent types
STAGE_DEFINITIONS = {
    1: {
        "name": "Investigation",
        "skills_file": "flows/investigation_skills.md",
        "agent_type": "investigation",
        "agent_identity": "Senior Data Engineer / Data Detective",
        "description": "Diagnose data ingestion failures and identify root causes",
        "tools_used": [
            "read_logs",
            "query_duckdb", 
            "inspect_file",
            "check_schema",
            "get_ingestion_status",
            "send_slack_alert"
        ],
        "trigger_phrases": [
            "investigate",
            "diagnose",
            "what went wrong",
            "find the error",
            "troubleshoot",
            "analyze the failure"
        ]
    },
    2: {
        "name": "Pipeline Builder",
        "skills_file": "agents/pipeline_builder/skills.md",
        "agent_type": "pipeline_builder",
        "agent_identity": "Data Pipeline Engineer",
        "description": "Generate cleaning pipelines to fix data quality issues",
        "tools_used": [
            "load_ideal_schema",
            "infer_source_schema",
            "compare_schemas",
            "generate_cleaning_pipeline",
            "write_file"
        ],
        "trigger_phrases": [
            "generate pipeline",
            "create cleaning code",
            "fix the data",
            "build transformation",
            "automatically fix",
            "self-healing",
            "generate SQL"
        ]
    },
    3: {
        "name": "Validation",
        "skills_file": "flows/validation_skills.md",
        "agent_type": "validation",
        "agent_identity": "Data Quality Assurance Engineer",
        "description": "Execute and validate generated cleaning pipelines",
        "tools_used": [
            "query_duckdb",
            "subprocess.run",
            "execute_pipeline"
        ],
        "trigger_phrases": [
            "validate the pipeline",
            "test the cleaning code",
            "verify the output",
            "check if it worked",
            "run validation",
            "confirm correctness",
            "quality assurance"
        ]
    }
}


def get_stage_definition(stage: int) -> Dict[str, Any]:
    """
    Get the definition for a specific stage.
    
    Args:
        stage: Stage number (1, 2, or 3)
        
    Returns:
        Dictionary with stage definition
    """
    return STAGE_DEFINITIONS.get(stage, {"error": f"Invalid stage: {stage}"})


def load_skills_file(file_path: str, project_root: Optional[Path] = None) -> str:
    """
    Load the content of a skills file.
    
    Args:
        file_path: Path to the skills file (relative to project root)
        project_root: Project root path (optional)
        
    Returns:
        Content of the skills file
    """
    root = project_root or PROJECT_ROOT
    full_path = root / file_path
    
    if not full_path.exists():
        return f"Skills file not found: {full_path}"
    
    with open(full_path, 'r') as f:
        return f.read()


def load_skills_for_agent(stage: int, project_root: Optional[Path] = None) -> str:
    """
    Load the skills content for a specific agent stage.
    
    This is useful for providing the complete skills context to an agent
    when it's operating in a specific stage.
    
    Args:
        stage: Stage number (1, 2, or 3)
        project_root: Project root path (optional)
        
    Returns:
        Complete skills content for that stage
    """
    definition = get_stage_definition(stage)
    
    if "error" in definition:
        return definition["error"]
    
    skills_file = definition.get("skills_file")
    if not skills_file:
        return f"No skills file defined for stage {stage}"
    
    return load_skills_file(skills_file, project_root)


def get_agent_context(stage: int, project_root: Optional[Path] = None) -> str:
    """
    Get a complete context string for an agent operating in a specific stage.
    
    This combines the master SKILLS.md index with the stage-specific skills
    to give the agent a complete picture of the workflow and its role.
    
    Args:
        stage: Stage number (1, 2, or 3)
        project_root: Project root path (optional)
        
    Returns:
        Formatted context string for the agent
    """
    root = project_root or PROJECT_ROOT
    
    # Load master skills index
    master_skills = load_skills_file("SKILLS.md", root)
    
    # Load stage-specific skills
    stage_skills = load_skills_for_agent(stage, root)
    
    # Get stage definition
    definition = get_stage_definition(stage)
    
    # Build context
    context_parts = []
    
    # Header
    context_parts.append("=" * 80)
    context_parts.append(f"AGENT CONTEXT: Stage {stage} - {definition.get('name', 'Unknown')}")
    context_parts.append("=" * 80)
    context_parts.append("")
    
    # Stage information
    context_parts.append(f"## Your Role")
    context_parts.append(f"You are a **{definition.get('agent_identity', 'AI Assistant')}**.")
    context_parts.append(f"Your mission: {definition.get('description', '')}")
    context_parts.append("")
    
    # Workflow context
    context_parts.append(f"## Workflow Context")
    context_parts.append(f"You are operating in **Stage {stage}** of a 3-stage autonomous workflow:")
    context_parts.append(f"  - Stage 1: Investigation (Find what's wrong)")
    context_parts.append(f"  - Stage 2: Pipeline Builder (Generate a fix)")
    context_parts.append(f"  - Stage 3: Validation (Verify the fix works)")
    context_parts.append("")
    
    # Stage-specific guidance
    context_parts.append(f"## Your Stage Skills")
    context_parts.append(f"The following guidance is specific to your stage:")
    context_parts.append("")
    context_parts.append("-" * 80)
    context_parts.append(stage_skills)
    context_parts.append("-" * 80)
    context_parts.append("")
    
    # Complete workflow overview
    context_parts.append(f"## Complete Workflow Overview")
    context_parts.append("")
    context_parts.append(master_skills)
    
    # Project information
    context_parts.append("")
    context_parts.append("=" * 80)
    context_parts.append("PROJECT INFORMATION")
    context_parts.append("=" * 80)
    context_parts.append(f"Project Root: {root}")
    context_parts.append(f"Stage: {stage}")
    context_parts.append(f"Agent Type: {definition.get('agent_type', 'unknown')}")
    context_parts.append("")
    
    # Available tools
    tools = definition.get("tools_used", [])
    if tools:
        context_parts.append(f"## Available Tools for This Stage")
        context_parts.append("")
        for tool in tools:
            context_parts.append(f"- `{tool}`")
        context_parts.append("")
    
    return "\n".join(context_parts)


def get_complete_workflow_context(project_root: Optional[Path] = None) -> str:
    """
    Get a complete context that includes all stage skills.
    
    Useful for agents that need to understand the entire workflow
    (e.g., a coordinator agent that manages multiple stages).
    
    Args:
        project_root: Project root path (optional)
        
    Returns:
        Complete workflow context string
    """
    root = project_root or PROJECT_ROOT
    
    parts = []
    
    # Load master skills
    master_skills = load_skills_file("SKILLS.md", root)
    parts.append(master_skills)
    
    # Add all stage skills
    parts.append("\n\n" + "=" * 80)
    parts.append("STAGE-SPECIFIC SKILLS")
    parts.append("=" * 80)
    
    for stage in [1, 2, 3]:
        definition = get_stage_definition(stage)
        skills = load_skills_for_agent(stage, root)
        
        parts.append(f"\n\n{'=' * 80}")
        parts.append(f"STAGE {stage}: {definition.get('name', 'Unknown').upper()}")
        parts.append("=" * 80)
        parts.append(skills)
    
    return "\n".join(parts)


def get_stage_transition_context(from_stage: int, to_stage: int, 
                                   findings: Optional[str] = None, 
                                   project_root: Optional[Path] = None) -> str:
    """
    Get context for transitioning between stages.
    
    This provides the context needed when one agent hands off to another.
    
    Args:
        from_stage: Current stage number
        to_stage: Next stage number
        findings: Optional findings to include in handoff
        project_root: Project root path (optional)
        
    Returns:
        Handoff context string
    """
    from_def = get_stage_definition(from_stage)
    to_def = get_stage_definition(to_stage)
    
    root = project_root or PROJECT_ROOT
    
    parts = []
    parts.append("=" * 80)
    parts.append(f"STAGE TRANSITION: {from_def.get('name', '')} → {to_def.get('name', '')}")
    parts.append("=" * 80)
    parts.append("")
    
    parts.append(f"**From:** Stage {from_stage} ({from_def.get('name', '')})")
    parts.append(f"**To:** Stage {to_stage} ({to_def.get('name', '')})")
    parts.append("")
    
    if findings:
        parts.append("## Findings from Previous Stage")
        parts.append(findings)
        parts.append("")
    
    parts.append("## Next Stage Context")
    next_skills = load_skills_for_agent(to_stage, root)
    parts.append(next_skills)
    
    return "\n".join(parts)


def load_skills_for_nanobot_config(stage: int, project_root: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load skills and format them for use in Nanobot configuration.
    
    This returns a dictionary that can be used in Nanobot's skills/context configuration.
    
    Args:
        stage: Stage number (1, 2, or 3)
        project_root: Project root path (optional)
        
    Returns:
        Dictionary formatted for Nanobot config
    """
    definition = get_stage_definition(stage)
    skills_content = load_skills_for_agent(stage, project_root)
    
    return {
        "stage": stage,
        "name": definition.get("name", ""),
        "agent_type": definition.get("agent_type", ""),
        "agent_identity": definition.get("agent_identity", ""),
        "description": definition.get("description", ""),
        "skills_content": skills_content,
        "tools": definition.get("tools_used", []),
        "trigger_phrases": definition.get("trigger_phrases", []),
        "workflow_position": stage
    }


def create_nanobot_stage_config(stage: int, model: str = "gpt-4o-mini",
                                project_root: Optional[Path] = None) -> Dict[str, Any]:
    """
    Create a Nanobot configuration for a specific stage.
    
    This can be used to create specialized agents for each stage.
    
    Args:
        stage: Stage number (1, 2, or 3)
        model: LLM model to use
        project_root: Project root path (optional)
        
    Returns:
        Nanobot agent configuration dictionary
    """
    definition = get_stage_definition(stage)
    skills_loader = __import__('skills', fromlist=['SkillLoader'])
    loader = skills_loader.SkillLoader(project_root)
    stage_summary = loader.get_stage_summary(stage)
    
    # Load the full skills content
    skills_content = load_skills_for_agent(stage, project_root)
    
    # Create system prompt
    system_prompt = f"""You are an autonomous data ingestion agent operating in **Stage {stage}: {definition.get('name', '')}**.

{stage_summary.get('mission', '')}

## YOUR ROLE
- You are a **{definition.get('agent_identity', '')}**
- You must be thorough and methodical
- Do not stop until your mission is complete
- Use the tools available to you
- Follow the guidance in your stage skills

## SKILLS CONTEXT
{skills_content}

## IMPORTANT
- Always start by understanding the context
- Use tools to verify information
- Document your findings clearly
- Handoff to the next stage when complete
"""
    
    config = {
        "name": f"stage_{stage}_{definition.get('agent_type', 'agent')}",
        "description": definition.get("description", ""),
        "model": model,
        "max_iterations": 20,
        "temperature": 0.3,
        "system_prompt": system_prompt,
        "context_files": [
            str((project_root or PROJECT_ROOT) / "SKILLS.md"),
            str((project_root or PROJECT_ROOT) / stage_summary.get("path", ""))
        ],
        "stage": stage,
        "tools": definition.get("tools_used", [])
    }
    
    return config


def get_tool_registration_info(stage: int) -> Dict[str, Any]:
    """
    Get information about which tools should be registered for a stage.
    
    Args:
        stage: Stage number (1, 2, or 3)
        
    Returns:
        Dictionary with tool registration information
    """
    definition = get_stage_definition(stage)
    
    # Map stage tools to actual tool implementations
    tool_mapping = {
        # Stage 1: Investigation tools
        1: {
            "read_logs": {
                "module": "flows.nanobot_tools",
                "function": "read_logs",
                "description": "Read application logs to find error details"
            },
            "query_duckdb": {
                "module": "flows.nanobot_tools",
                "function": "query_duckdb",
                "description": "Execute SQL queries against the DuckDB database"
            },
            "inspect_file": {
                "module": "flows.nanobot_tools",
                "function": "inspect_file",
                "description": "Inspect source data files for metadata and sample data"
            },
            "check_schema": {
                "module": "flows.nanobot_tools",
                "function": "check_schema",
                "description": "Validate data against expected schema"
            },
            "get_ingestion_status": {
                "module": "flows.nanobot_tools",
                "function": "get_ingestion_status",
                "description": "Get current status of the ingestion pipeline"
            },
            "send_slack_alert": {
                "module": "flows.nanobot_tools",
                "function": "send_slack_alert",
                "description": "Send investigation results to Slack"
            }
        },
        # Stage 2: Pipeline Builder tools
        2: {
            "load_ideal_schema": {
                "module": "agents.pipeline_builder.tools",
                "function": "load_ideal_schema",
                "description": "Load the ideal schema definition from YAML"
            },
            "infer_source_schema": {
                "module": "agents.pipeline_builder.tools",
                "function": "infer_source_schema",
                "description": "Infer schema from source CSV file"
            },
            "compare_schemas": {
                "module": "agents.pipeline_builder.tools",
                "function": "compare_schemas",
                "description": "Compare source and ideal schemas, identify mismatches"
            },
            "generate_cleaning_pipeline": {
                "module": "agents.pipeline_builder.tools",
                "function": "generate_cleaning_pipeline",
                "description": "Generate complete cleaning pipeline (SQL + Python)"
            },
            "write_file": {
                "module": "builtins",
                "function": "open",
                "description": "Write content to a file"
            }
        },
        # Stage 3: Validation tools (uses many from stage 1)
        3: {
            "query_duckdb": {
                "module": "flows.nanobot_tools",
                "function": "query_duckdb",
                "description": "Execute SQL queries against the DuckDB database"
            },
            "inspect_file": {
                "module": "flows.nanobot_tools",
                "function": "inspect_file",
                "description": "Inspect files for validation"
            },
            "check_schema": {
                "module": "flows.nanobot_tools",
                "function": "check_schema",
                "description": "Validate cleaned data against schema"
            }
        }
    }
    
    return {
        "stage": stage,
        "tools": tool_mapping.get(stage, {}),
        "tool_count": len(tool_mapping.get(stage, {}))
    }


def register_stage_tools(bot, stage: int, project_root: Optional[Path] = None) -> Dict[str, Any]:
    """
    Register tools for a specific stage with a Nanobot instance.
    
    Args:
        bot: Nanobot instance
        stage: Stage number (1, 2, or 3)
        project_root: Project root path (optional)
        
    Returns:
        Dictionary with registration results
    """
    results = {
        "stage": stage,
        "tools_registered": [],
        "errors": []
    }
    
    tool_info = get_tool_registration_info(stage)
    tools_config = tool_info.get("tools", {})
    
    # Setup Python path
    root = project_root or PROJECT_ROOT
    sys.path.insert(0, str(root))
    os.environ["PYTHONPATH"] = str(root)
    
    for tool_name, tool_config in tools_config.items():
        try:
            module_name = tool_config.get("module")
            function_name = tool_config.get("function")
            description = tool_config.get("description", "")
            
            # Import the function
            if module_name == "builtins":
                # Handle built-in functions specially
                if function_name == "open":
                    # For write_file, we'll use a wrapper
                    def write_file(path: str, content: str) -> bool:
                        """Write content to a file."""
                        with open(path, 'w') as f:
                            f.write(content)
                        return True
                    func = write_file
                else:
                    func = getattr(__builtins__, function_name)
            else:
                # Import from module
                module = __import__(module_name, fromlist=[function_name])
                func = getattr(module, function_name)
            
            # Register with bot
            bot.register_tool(
                name=tool_name,
                description=description,
                func=func
            )
            
            results["tools_registered"].append(tool_name)
            
        except Exception as e:
            results["errors"].append({
                "tool": tool_name,
                "error": str(e)
            })
    
    return results
