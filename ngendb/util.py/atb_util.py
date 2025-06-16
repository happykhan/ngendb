def get_assembly_url(sample_id):
    # sample id should be a sam*
    # return the assembly url for the sample id
    # The sample_id should be something like 'SAMD00000344'
    base_url = 'https://allthebacteria-assemblies.s3.eu-west-2.amazonaws.com/'
    if not sample_id.lower().startswith('sam'):
        raise ValueError("sample_id should start with 'sam'")
    return f"{base_url}{sample_id}.fa.gz"

