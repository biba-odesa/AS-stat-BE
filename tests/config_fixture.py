"""Small unified-config fixture builder for loopback-only integration tests."""
def write_config(path, ports, **overrides):
    links=path.parent/'links'
    links.write_text('127.0.0.1 1 link First ff0000 999\n127.0.0.2 1 second Second 0000ff 999\n')
    values=dict(netflow9_ports=','.join(map(str,ports)),bind_address='127.0.0.1',
        exporters='{'+','.join(f'{p}:127.0.0.1' for p in ports)+'}',
        samplerate='{127.0.0.1:100}',exported_counters='{127.0.0.1:sampled}',
        knownlinks_file=str(links),replace_asn='64496',
        private_asn_ranges='{64512,65534},{4200000000,4294967294}',exclude_asn='64496',
        windows_output='none', output=str(path.parent/'final.json'))
    values.update(overrides)
    path.write_text(''.join(f'{k} = {v}\n' for k,v in values.items()))
