import argparse
import json
import sys
from pathlib import Path

import polars as pl

from .pipeline import PipelineError, run
from .synthetic import generate


class RussianParser(argparse.ArgumentParser):
    def __init__(self,*args,**kwargs):
        kwargs['add_help']=False
        super().__init__(*args,**kwargs)
        self.add_argument('-h','--help',action='help',help='показать справку')

    def format_help(self):
        return super().format_help().replace('usage: ','Использование: ').replace('options:','Параметры:')

    def error(self,message):
        self.exit(2,'Ошибка параметров. Проверьте значения; справка: --help.\n')


LABELS = {"events":"Синтетических запросов","duplicates":"Добавлено дублей","invalid_rows":"Добавлено ошибочных строк",
          "seed":"Начальное значение генератора","input_rows":"Входных строк","accepted_rows":"Уникальных запросов",
          "duplicate_rows":"Пропущено дублей","rejected_rows":"Отклонено строк","partitions":"Разделов по дате UTC",
          "hourly_groups":"Почасовых групп","server_errors":"Ошибок сервера 5xx","client_errors":"Ошибок клиента 4xx",
          "slow_ms":"Порог медленного запроса, мс"}


def main() -> int:
    for stream in (sys.stdout,sys.stderr):
        if hasattr(stream,'reconfigure'):stream.reconfigure(encoding='utf-8')
    parser=RussianParser(prog='python -m logs_pipeline',description='JSONL → проверенные журналы, разделы Parquet и почасовая статистика на Polars')
    commands=parser.add_subparsers(dest='command',required=True,title='Команды')
    demo=commands.add_parser('demo',help='создать синтетические журналы')
    demo.add_argument('--input',type=Path,default=Path('workspace/input'),help='пустая папка для демо')
    demo.add_argument('--rows',type=int,default=10_000,help='число запросов: 10–90 000')
    demo.add_argument('--seed',type=int,default=42,help='начальное значение генератора')
    execute=commands.add_parser('run',help='полностью пересобрать снимок и статистику')
    execute.add_argument('--input',type=Path,default=Path('workspace/input'),help='папка JSONL')
    execute.add_argument('--output',type=Path,default=Path('workspace/output'),help='папка результата')
    execute.add_argument('--slow-ms',type=float,default=500,help='порог медленного запроса в мс')
    for command in (demo,execute):command.add_argument('--json',action='store_true',help='результат JSON для автоматизации')
    args=parser.parse_args()
    try:
        result=generate(args.input,args.rows,args.seed) if args.command=='demo' else run(args.input,args.output,args.slow_ms)
        if args.json:print(json.dumps(result,ensure_ascii=False,indent=2))
        else:
            print('Демоданные готовы.' if args.command=='demo' else 'Снимок пересобран. Откройте preview.md в папке результата.')
            for key,value in result.items():print(f'{LABELS[key]}: {value}')
        return 0
    except (PipelineError,OSError,UnicodeError,pl.exceptions.PolarsError) as exc:
        message=str(exc) if isinstance(exc,PipelineError) else 'Не удалось прочитать данные или записать результат. Проверьте права и соответствие JSON-схеме.'
        print('Ошибка: '+message,file=sys.stderr)
        return 1


if __name__=='__main__':
    raise SystemExit(main())
