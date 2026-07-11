"""
Skills Module for Loops Data Ingestion Project

This module provides a centralized way to load and register skills for AI agents.
It supports loading skills from markdown files and registering them with Nanobot.

Skills are organized by stage:
- Stage 1: Investigation (flows/investigation_skills.md)
- Stage 2: Pipeline Builder (agents/pipeline_builder/skills.md)
- Stage 3: Validation (flows/validation_skills.md)
- Shared: General skills (SKILLS.md)

Usage:
    from skills import SkillLoader, register_all_skills
    
    # Load and register all skills
    register_all_skills(bot)
    
    # Or load specific stage skills
    loader = SkillLoader()
    investigation_skills = loader.load_stage_skills(1)
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
import yaml
import re


# Project configuration
PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
SKILLS_DIR = PROJECT_ROOT / "skills"


class SkillLoader:
    """
    Loads and parses skills from markdown files.
    
    Skills files are markdown documents that contain structured guidance
    for AI agents. This class extracts actionable information from them.
    """
    
    # Stage to skills file mapping
    STAGE_SKILLS = {
        1: "flows/investigation_skills.md",
        2: "agents/pipeline_builder/skills.md",
        3: "flows/validation_skills.md",
    }
    
    # All skills files
    ALL_SKILLS_FILES = [
        "SKILLS.md",  # Master index
        "flows/investigation_skills.md",
        "agents/pipeline_builder/skills.md",
        "flows/validation_skills.md",
    ]
    
    def __init__(self, project_root: Optional[Path] = None):
        """
        Initialize the skill loader.
        
        Args:
            project_root: Root directory of the project. Defaults to PROJECT_ROOT.
        """
        self.project_root = project_root or PROJECT_ROOT
        self._skills_cache: Dict[str, Dict[str, Any]] = {}
    
    def load_skills_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load a skills markdown file and extract structured information.
        
        Args:
            file_path: Path to the skills markdown file (relative to project root)
            
        Returns:
            Dictionary containing:
            - name: Skill name
            - description: Description
            - stage: Stage number (1, 2, 3)
            - tools: List of tools mentioned
            - triggers: List of trigger phrases
            - workflow: Structured workflow steps
            - content: Full markdown content
        """
        full_path = self.project_root / file_path
        
        if not full_path.exists():
            return {
                "error": f"Skills file not found: {file_path}",
                "path": str(full_path)
            }
        
        # Check cache
        if file_path in self._skills_cache:
            return self._skills_cache[file_path]
        
        with open(full_path, 'r') as f:
            content = f.read()
        
        # Parse the markdown content
        skills_data = self._parse_skills_markdown(content, file_path)
        
        # Cache and return
        self._skills_cache[file_path] = skills_data
        return skills_data
    
    def _parse_skills_markdown(self, content: str, file_path: str) -> Dict[str, Any]:
        """
        Parse a skills markdown file and extract structured data.
        """
        skills = {
            "path": file_path,
            "content": content,
            "name": self._extract_title(content),
            "description": self._extract_description(content),
            "stage": self._extract_stage(content, file_path),
            "agent_identity": self._extract_agent_identity(content),
            "mission": self._extract_mission(content),
            "tools": self._extract_tools(content),
            "triggers": self._extract_triggers(content),
            "workflow": self._extract_workflow(content),
            "checklists": self._extract_checklists(content),
            "code_examples": self._extract_code_blocks(content),
            "tables": self._extract_tables(content),
        }
        
        return skills
    
    def _extract_title(self, content: str) -> str:
        """Extract the title (first H1) from markdown."""
        match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
        if match:
            return match.group(1).strip()
        return "Untitled Skills"
    
    def _extract_description(self, content: str) -> str:
        """Extract the description (content after title, before first H2)."""
        # Remove title
        content_no_title = re.sub(r'^#\s+.+$', '', content, flags=re.MULTILINE, count=1)
        # Get content before first H2 or 3 dashes
        match = re.search(r'^([\s\S]+?)(?:^##|^---)', content_no_title, re.MULTILINE)
        if match:
            return match.group(1).strip()
        return content_no_title[:200].strip() + "..."
    
    def _extract_stage(self, content: str, file_path: str) -> Optional[int]:
        """Extract the stage number from content or file path."""
        # Try to extract from content
        stage_match = re.search(r'Stage\s+(\d+)', content)
        if stage_match:
            return int(stage_match.group(1))
        
        # Try to infer from file path
        if "investigation" in file_path:
            return 1
        elif "pipeline_builder" in file_path:
            return 2
        elif "validation" in file_path:
            return 3
        
        return None
    
    def _extract_agent_identity(self, content: str) -> str:
        """Extract the agent identity/role from content."""
        match = re.search(r'You are:\s*(.+)\n', content)
        if match:
            return match.group(1).strip()
        return "AI Assistant"
    
    def _extract_mission(self, content: str) -> str:
        """Extract the mission statement from content."""
        match = re.search(r'Your mission:\s*(.+?)(?:\n\n|\n##)', content, re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""
    
    def _extract_tools(self, content: str) -> List[str]:
        """Extract all tool names mentioned in the content."""
        # Known valid tools from the project
        known_tools = {
            # Stage 1: Investigation tools
            'read_logs', 'query_duckdb', 'inspect_file', 'check_schema',
            'get_ingestion_status', 'send_slack_alert',
            # Stage 2: Pipeline Builder tools
            'load_ideal_schema', 'infer_source_schema', 'compare_schemas',
            'generate_cleaning_pipeline', 'write_file',
            # Stage 3: Validation tools (reuses some from stage 1)
            'execute_pipeline',
        }
        
        tools = set()
        
        # Look for Tools Used / Tools to use / Tools Available sections (most reliable)
        tool_section_patterns = [
            r'Tools Used:\s*([\s\S]+?)(?:\n\n|\n##|\n\`\`\`)',
            r'Tools to use:\s*([\s\S]+?)(?:\n\n|\n##|\n\`\`\`)',
            r'Tools Available:\s*([\s\S]+?)(?:\n\n|\n##)',
            r'tools:\s*([\s\S]+?)(?:\n\n|\n##)',
        ]
        
        for pattern in tool_section_patterns:
            tool_section = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if tool_section:
                tool_text = tool_section.group(1)
                # Extract backtick-wrapped tool names
                pattern2 = r'`([a-z_]+)`'
                matches = re.findall(pattern2, tool_text)
                for match in matches:
                    if match in known_tools:
                        tools.add(match)
        
        # Look for tool names in code blocks (backticks) - function calls
        pattern = r'`([a-z_]+)\([^`]*\)`'
        matches = re.findall(pattern, content)
        for match in matches:
            if match in known_tools:
                tools.add(match)
        
        # Look for tool names in python code blocks
        pattern = r'```python\s*\n[\s\S]*?([a-z_]+)\([^\n)]*\)[\s\S]*?```'
        matches = re.findall(pattern, content, re.DOTALL)
        for match in matches:
            if match in known_tools:
                tools.add(match)
        
        # Look for numbered list items: 1. `tool_name` or 1. **`tool_name`**
        pattern = r'\d+\.\s*`([a-z_]+)`'
        matches = re.findall(pattern, content)
        for match in matches:
            if match in known_tools:
                tools.add(match)
        
        # Look for bolded tool names
        pattern = r'\*\*`([a-z_]+)`\*\*'
        matches = re.findall(pattern, content)
        for match in matches:
            if match in known_tools:
                tools.add(match)
        
        # Look for list items with tool names: - `tool_name` or - **`tool_name`**
        pattern = r'-\s+`([a-z_]+)`'
        matches = re.findall(pattern, content)
        for match in matches:
            if match in known_tools:
                tools.add(match)
        
        return sorted(list(tools))
    
    def _extract_triggers(self, content: str) -> List[str]:
        """Extract trigger phrases from content."""
        triggers = []
        
        # Look for Trigger Phrases section
        trigger_section = re.search(r'Trigger Phrases\s*([\s\S]+?)(?:\n\n|\n##|$)', content, re.DOTALL)
        if trigger_section:
            trigger_text = trigger_section.group(1)
            # Extract list items
            triggers = re.findall(r'-\s*"([^"]+)"', trigger_text)
        
        return triggers
    
    def _extract_workflow(self, content: str) -> Dict[str, Any]:
        """Extract workflow steps from content."""
        workflow = {}
        
        # Extract Phase/Step sections
        phase_pattern = r'(?:Phase|Step)\s+(\d+):\s+(.+?)\n'
        matches = re.findall(phase_pattern, content)
        
        for phase_num, phase_title in matches:
            workflow[f"phase_{phase_num}"] = {
                "title": phase_title.strip(),
                "steps": []
            }
        
        # Extract numbered steps within each phase
        for phase_key in workflow.keys():
            phase_section = re.search(
                rf'(?:Phase|Step)\s+{phase_key.split("_")[1]}:\s+.+?\n([\s\S]+?)(?:\n\n\n|\n##|$)',
                content,
                re.DOTALL
            )
            if phase_section:
                steps_text = phase_section.group(1)
                # Extract numbered or bullet list items
                step_matches = re.findall(r'\d+\.\s+(.+?)(?:\n|$)', steps_text)
                for step in step_matches:
                    workflow[phase_key]["steps"].append(step.strip())
        
        return workflow
    
    def _extract_checklists(self, content: str) -> Dict[str, List[str]]:
        """Extract checklists from content."""
        checklists = {}
        
        # Find checklist tables (ASCII art)
        checklist_pattern = r'┌.*?┐\s*\n│\s*([^│]+?)\s*│\s*\n├.*?┤\s*\n(.*?)\n└.*?┘'
        matches = re.findall(checklist_pattern, content, re.DOTALL)
        
        for title, items in matches:
            checklist_name = title.strip()
            checklist_items = []
            
            # Extract items from table rows
            for line in items.split('\n'):
                if '│' in line:
                    item_match = re.search(r'│\s*\[[ xX]\]\s*(.*?)\s*│', line)
                    if item_match:
                        checklist_items.append(item_match.group(1).strip())
            
            if checklist_items:
                checklists[checklist_name] = checklist_items
        
        return checklists
    
    def _extract_code_blocks(self, content: str) -> Dict[str, str]:
        """Extract code blocks from content."""
        code_blocks = {}
        
        # Python code blocks
        pattern = r'```python\s*\n([\s\S]+?)```'
        matches = re.findall(pattern, content)
        for i, code in enumerate(matches):
            code_blocks[f"python_{i}"] = code.strip()
        
        # SQL code blocks
        pattern = r'```sql\s*\n([\s\S]+?)```'
        matches = re.findall(pattern, content)
        for i, code in enumerate(matches):
            code_blocks[f"sql_{i}"] = code.strip()
        
        # JSON code blocks
        pattern = r'```json\s*\n([\s\S]+?)```'
        matches = re.findall(pattern, content)
        for i, code in enumerate(matches):
            code_blocks[f"json_{i}"] = code.strip()
        
        # Bash code blocks
        pattern = r'```bash\s*\n([\s\S]+?)```'
        matches = re.findall(pattern, content)
        for i, code in enumerate(matches):
            code_blocks[f"bash_{i}"] = code.strip()
        
        return code_blocks
    
    def _extract_tables(self, content: str) -> List[Dict[str, List[str]]]:
        """Extract markdown tables from content."""
        tables = []
        
        # Markdown table pattern
        table_pattern = r'\n(\|.*?\|)\s*\n(\|.*?\|)\s*\n(\|.*?\|)(?:\s*\n(\|.*?\|))*'
        matches = re.findall(table_pattern, content, re.MULTILINE)
        
        for match in matches:
            table_data = {
                "headers": [],
                "rows": []
            }
            
            # Parse headers (first row)
            if match[0]:
                headers = [h.strip() for h in match[0].split('|') if h.strip()]
                table_data["headers"] = headers
            
            # Parse rows
            for row_line in match[1:]:
                if row_line:
                    # Skip separator rows (with dashes)
                    if '---' not in row_line:
                        row = [r.strip() for r in row_line.split('|') if r.strip()]
                        if row and len(row) == len(table_data["headers"]):
                            table_data["rows"].append(row)
            
            if table_data["headers"] and table_data["rows"]:
                tables.append(table_data)
        
        return tables
    
    def load_stage_skills(self, stage: int) -> Dict[str, Any]:
        """
        Load skills for a specific stage.
        
        Args:
            stage: Stage number (1, 2, or 3)
            
        Returns:
            Skills data for that stage
        """
        if stage not in self.STAGE_SKILLS:
            return {"error": f"Invalid stage: {stage}. Must be 1, 2, or 3."}
        
        return self.load_skills_file(self.STAGE_SKILLS[stage])
    
    def load_all_skills(self) -> Dict[str, Dict[str, Any]]:
        """
        Load all skills files.
        
        Returns:
            Dictionary mapping file paths to skills data
        """
        all_skills = {}
        for skills_file in self.ALL_SKILLS_FILES:
            skills_data = self.load_skills_file(skills_file)
            if "error" not in skills_data:
                all_skills[skills_file] = skills_data
        return all_skills
    
    def get_stage_summary(self, stage: int) -> Dict[str, Any]:
        """
        Get a summary of skills for a specific stage.
        
        Args:
            stage: Stage number (1, 2, or 3)
            
        Returns:
            Summary with key information
        """
        skills = self.load_stage_skills(stage)
        
        if "error" in skills:
            return skills
        
        return {
            "stage": stage,
            "name": skills.get("name", ""),
            "agent_identity": skills.get("agent_identity", ""),
            "mission": skills.get("mission", ""),
            "tools": skills.get("tools", []),
            "triggers": skills.get("triggers", []),
            "tool_count": len(skills.get("tools", [])),
            "checklist_count": len(skills.get("checklists", {})),
            "code_example_count": len(skills.get("code_examples", {})),
        }


def register_all_skills(bot, project_root: Optional[Path] = None) -> Dict[str, Any]:
    """
    Register all skills with a Nanobot instance.
    
    This function loads all skills files and registers their tools with the bot.
    
    Args:
        bot: Nanobot instance to register skills with
        project_root: Project root path (optional)
        
    Returns:
        Dictionary with registration results
    """
    loader = SkillLoader(project_root)
    results = {
        "stages_loaded": [],
        "tools_registered": [],
        "errors": []
    }
    
    try:
        # Load all skills
        all_skills = loader.load_all_skills()
        
        # Register tools from each stage
        for stage in [1, 2, 3]:
            skills = loader.load_stage_skills(stage)
            if "error" not in skills:
                results["stages_loaded"].append(stage)
                
                # Get tools from this stage
                tools = skills.get("tools", [])
                
                # Try to register each tool if it exists in the registry
                # For now, just collect the tools - actual registration
                # requires the bot to have the tool functions available
                results["tools_registered"].extend(tools)
        
        # Log success
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Loaded {len(results['stages_loaded'])} stages with {len(results['tools_registered'])} tools")
        
    except Exception as e:
        results["errors"].append(str(e))
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error loading skills: {e}")
    
    return results


def load_stage_context(stage: int, project_root: Optional[Path] = None) -> str:
    """
    Load the full content of a stage's skills file for use as context.
    
    This is useful for providing agents with the complete guidance for their stage.
    
    Args:
        stage: Stage number (1, 2, or 3)
        project_root: Project root path (optional)
        
    Returns:
        Full markdown content of the skills file
    """
    loader = SkillLoader(project_root)
    skills_file = loader.STAGE_SKILLS.get(stage)
    
    if not skills_file:
        return f"Invalid stage: {stage}"
    
    full_path = (project_root or PROJECT_ROOT) / skills_file
    
    if not full_path.exists():
        return f"Skills file not found: {full_path}"
    
    with open(full_path, 'r') as f:
        return f.read()


def get_workflow_overview(project_root: Optional[Path] = None) -> str:
    """
    Get a complete overview of all stages and their skills.
    
    Useful for providing agents with context about the entire workflow.
    
    Args:
        project_root: Project root path (optional)
        
    Returns:
        Formatted overview string
    """
    loader = SkillLoader(project_root)
    
    overview = []
    overview.append("# Multi-Stage Workflow Overview\n")
    overview.append("This project uses a 3-stage autonomous workflow for data ingestion troubleshooting.\n")
    
    for stage in [1, 2, 3]:
        summary = loader.get_stage_summary(stage)
        if "error" not in summary:
            overview.append(f"\n## Stage {stage}: {summary['name']}\n")
            overview.append(f"- **Agent**: {summary['agent_identity']}\n")
            overview.append(f"- **Mission**: {summary['mission']}\n")
            overview.append(f"- **Tools**: {', '.join(summary['tools'])}\n")
            overview.append(f"- **Triggers**: {', '.join(summary['triggers'])}\n")
    
    # Add workflow diagram
    overview.append("\n## Workflow\n")
    overview.append("```\n")
    overview.append("Stage 1: Investigation → Stage 2: Pipeline Builder → Stage 3: Validation\n")
    overview.append("```\n")
    
    return "".join(overview)


# Convenience functions
def load_investigation_skills(project_root: Optional[Path] = None) -> Dict[str, Any]:
    """Load Stage 1 (Investigation) skills."""
    loader = SkillLoader(project_root)
    return loader.load_stage_skills(1)


def load_pipeline_builder_skills(project_root: Optional[Path] = None) -> Dict[str, Any]:
    """Load Stage 2 (Pipeline Builder) skills."""
    loader = SkillLoader(project_root)
    return loader.load_stage_skills(2)


def load_validation_skills(project_root: Optional[Path] = None) -> Dict[str, Any]:
    """Load Stage 3 (Validation) skills."""
    loader = SkillLoader(project_root)
    return loader.load_stage_skills(3)
