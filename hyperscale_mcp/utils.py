import os
import platform

def get_default_shell():
    """Get the default shell path for the current system."""
    if platform.system() == "Windows":
      # Windows is not supported yet
      raise NotImplementedError("Windows is not supported yet")
    
    # For Unix-like systems
    shell = os.environ.get("SHELL")
    if shell and os.path.exists(shell):
        return shell
    
    # Try common Unix shells in order of preference
    for shell in ["/bin/bash", "/bin/sh", "/bin/zsh"]:
        if os.path.exists(shell):
            return shell
    
    raise RuntimeError("No suitable shell found")