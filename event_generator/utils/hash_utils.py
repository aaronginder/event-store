import hashlib
import uuid

def generate_entity_id(seed=None):
    """Generate a hash-based entity ID.
    
    Args:
        seed: Optional seed value to create deterministic hashes
        
    Returns:
        A string hash value
    """
    if seed:
        # Create deterministic hash based on seed
        return hashlib.md5(str(seed).encode()).hexdigest()
    else:
        # Create random UUID
        return str(uuid.uuid4())