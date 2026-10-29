"""Observe official ty subprocess results without changing calls or return values."""
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys

if Path(sys.argv[0]).name == 'run-ty.py':
    _original_run = subprocess.run

    def _observed_run(*args, **kwargs):
        result = _original_run(*args, **kwargs)
        command = args[0] if args else kwargs.get('args')
        if isinstance(command, str) and 'ty check ' in command:
            match = re.search(r'--config-file=\S*/\.ty-([^\s]+)\.toml', command)
            env = match.group(1) if match else 'unknown'
            try:
                out = Path('<EVIDENCE>')
                stdout = result.stdout or ''
                stderr = result.stderr or ''
                (out / f'ty-{env}.stdout.json').write_text(stdout, encoding='utf-8')
                (out / f'ty-{env}.stderr.log').write_text(stderr, encoding='utf-8')
                diagnostics = json.loads(stdout) if stdout.strip() else []
                record = {
                    'atUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'command': command, 'cwd': str(kwargs.get('cwd')),
                    'returncode': result.returncode,
                    'filesAnalyzed': sum(map(int, re.findall(r'^INFO Indexed (\d+) file\(s\)', stderr, re.MULTILINE))),
                    'errorCount': sum(d['severity'] == 'major' for d in diagnostics),
                    'warningCount': sum(d['severity'] != 'major' for d in diagnostics),
                    'diagnosticCount': len(diagnostics),
                    'observation': 'Original subprocess.run called once with unchanged positional and keyword arguments; original result returned unchanged.',
                }
                (out / f'ty-{env}.result.json').write_text(json.dumps(record, indent=2) + '\n')
            except Exception as error:
                print(f'OBSERVER_RECORD_FAILURE {env}: {error}', file=sys.stderr)
        return result

    subprocess.run = _observed_run
