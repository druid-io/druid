/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { render } from '@testing-library/react';
import { sqlParserFactory } from 'druid-query-toolkit';
import React from 'react';

import { SQL_FUNCTIONS, SyntaxDescription } from '../../../../lib/sql-function-doc';

import { QueryOutput } from './query-output';

describe('query output', () => {
  it('matches snapshot', () => {
    const parser = sqlParserFactory(
      SQL_FUNCTIONS.map((sqlFunction: SyntaxDescription) => {
        return sqlFunction.syntax.substr(0, sqlFunction.syntax.indexOf('('));
      }),
    );

    const parsedQuery = parser(`SELECT
  "language",
  COUNT(*) AS "Count", COUNT(DISTINCT "language") AS "dist_language", COUNT(*) FILTER (WHERE "language"= 'xxx') AS "language_filtered_count"
FROM "github"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY AND "language" != 'TypeScript'
GROUP BY 1
HAVING "Count" != 37392
ORDER BY "Count" DESC`);

    const queryOutput = (
      <QueryOutput
        runeMode={false}
        sqlOrderBy={() => null}
        sqlFilterRow={() => null}
        sqlExcludeColumn={() => null}
        loading={false}
        error="lol"
        queryResult={{
          header: ['language', 'Count', 'dist_language', 'language_filtered_count'],
          rows: [
            ['', 6881, 1, 0],
            ['JavaScript', 166, 1, 0],
            ['Python', 62, 1, 0],
            ['HTML', 46, 1, 0],
            ['Java', 42, 1, 0],
            ['C++', 28, 1, 0],
            ['Go', 24, 1, 0],
            ['Ruby', 20, 1, 0],
            ['C#', 14, 1, 0],
            ['C', 13, 1, 0],
            ['CSS', 13, 1, 0],
            ['Shell', 12, 1, 0],
            ['Makefile', 10, 1, 0],
            ['PHP', 9, 1, 0],
            ['Scala', 8, 1, 0],
            ['HCL', 6, 1, 0],
            ['Jupyter Notebook', 6, 1, 0],
            ['Smarty', 4, 1, 0],
            ['Elm', 4, 1, 0],
            ['Roff', 3, 1, 0],
            ['Dockerfile', 3, 1, 0],
            ['Rust', 3, 1, 0],
            ['Dart', 2, 1, 0],
            ['LLVM', 2, 1, 0],
            ['Objective-C', 2, 1, 0],
            ['Julia', 2, 1, 0],
            ['PowerShell', 2, 1, 0],
            ['Swift', 2, 1, 0],
            ['Nim', 2, 1, 0],
            ['XSLT', 1, 1, 0],
            ['Lua', 1, 1, 0],
            ['Vim script', 1, 1, 0],
            ['Vue', 1, 1, 0],
            ['Lasso', 1, 1, 0],
            ['Clojure', 1, 1, 0],
            ['OCaml', 1, 1, 0],
            ['Chapel', 1, 1, 0],
            ['Kotlin', 1, 1, 0],
          ],
        }}
        parsedQuery={parsedQuery}
      />
    );

    const { container } = render(queryOutput);
    expect(container.firstChild).toMatchSnapshot();
  });
});
