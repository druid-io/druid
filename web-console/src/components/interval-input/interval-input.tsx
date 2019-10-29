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

import { Button, InputGroup, Popover, Position } from '@blueprintjs/core';
import { DateRange, DateRangePicker } from '@blueprintjs/datetime';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import './interval-input.scss';

const CURRENT_YEAR = new Date().getUTCFullYear();

export interface IntervalInputProps {
  interval: string;
  placeholder: string | undefined;
  onValueChange: (interval: string) => void;
}

export interface IntervalInputState {
  currentInterval: string;
  dateRange: DateRange;
}

export class IntervalInput extends React.PureComponent<IntervalInputProps, IntervalInputState> {
  constructor(props: IntervalInputProps) {
    super(props);
    this.state = {
      currentInterval: this.props.interval,
      dateRange: this.parseInterval(this.props.interval),
    };
  }

  parseInterval(interval: string): DateRange {
    const dates = interval.split('/');
    if (dates.length !== 2 || !Date.parse(dates[0]) || !Date.parse(dates[1])) {
      return [undefined, undefined];
    }

    const startDateParts = dates[0].split('-');
    const endDateParts = dates[1].split('-');
    console.log(startDateParts);
    if (
      parseInt(startDateParts[0], 10) < CURRENT_YEAR - 20 ||
      parseInt(endDateParts[0], 10) > CURRENT_YEAR
    ) {
      return [undefined, undefined];
    }
    const startDate = new Date(dates[0]);
    const endDate = new Date(dates[1]);
    return [startDate, endDate];
  }

  parseDateRange(range: DateRange): string {
    const [startDate, endDate] = range;
    console.log(range);
    return `${startDate ? startDate.toISOString().substring(0, 19) : ''}/${
      endDate ? endDate.toISOString().substring(0, 19) : ''
    }`;
  }
  render() {
    const { currentInterval, dateRange } = this.state;
    const { onValueChange, placeholder } = this.props;
    return (
      <InputGroup
        value={`${currentInterval}`}
        placeholder={placeholder}
        rightElement={
          <div>
            <Popover
              popoverClassName={'calendar'}
              content={
                <DateRangePicker
                  timePrecision={'second'}
                  value={dateRange}
                  contiguousCalendarMonths={false}
                  onChange={(selectedRange: DateRange) => {
                    this.setState({ dateRange: selectedRange });
                    this.setState(
                      {
                        currentInterval: this.parseDateRange(selectedRange),
                      },
                      () => onValueChange(this.state.currentInterval),
                    );
                  }}
                />
              }
              position={Position.BOTTOM_RIGHT}
            >
              <Button rightIcon={IconNames.CALENDAR} />
            </Popover>
          </div>
        }
        onChange={(e: any) => {
          this.setState({ currentInterval: e.target.value }, () => {
            onValueChange(this.state.currentInterval);
          });
          this.setState({ dateRange: this.parseInterval(e.target.value) });
        }}
      />
    );
  }
}
