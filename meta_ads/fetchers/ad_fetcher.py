def process_creative(self, creative: Dict) -> Dict:
    """
    Process creative information into a flat structure.
    
    Args:
        creative: Creative dictionary from API
    """
    processed = {
        'creative_id': creative.get('id'),
        'creative_name': creative.get('name'),
        'body': creative.get('body'),
        'title': creative.get('title'),
        'call_to_action_type': creative.get('call_to_action_type'),
        'link_url': creative.get('link_url'),
        'image_url': creative.get('image_url'),
        'video_id': creative.get('video_id')
    }
    
    # Process platform customizations
    platform_custom = creative.get('platform_customizations', {})
    if platform_custom:
        for platform, settings in platform_custom.items():
            processed[f'{platform}_customization'] = str(settings)
    
    return processed