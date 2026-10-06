import { FormFieldType } from '@/components/dynamic-form';
import { TFunction } from 'i18next';

export const xwikiConstant = (t: TFunction) => [
  {
    label: 'Wiki Base URL',
    name: 'config.wiki_base',
    type: FormFieldType.Text,
    required: true,
    tooltip: t('setting.xwikiWikiBaseUrlTip'),
  },
  {
    label: 'Wiki ID',
    name: 'config.wiki',
    type: FormFieldType.Text,
    required: false,
    tooltip: t('setting.xwikiWikiIdTip'),
  },
  {
    label: 'Username',
    name: 'config.credentials.username',
    type: FormFieldType.Text,
    required: false,
    tooltip: t('setting.xwikiCredentialsTip'),
  },
  {
    label: 'Password / Token',
    name: 'config.credentials.password',
    type: FormFieldType.Password,
    required: false,
  },
  {
    label: 'Index Mode',
    name: 'config.index_mode',
    type: FormFieldType.Segmented,
    options: [
      { label: 'Everything', value: 'everything' },
      { label: 'Space', value: 'space' },
    ],
  },
  {
    name: 'config.space',
    label: 'Space Key',
    type: FormFieldType.Text,
    tooltip: t('setting.xwikiSpaceKeyTip'),
    customValidate: (val: string, formValues: any) => {
      const index_mode = formValues?.config?.index_mode;
      if (!val && index_mode === 'space') {
        return 'Space Key is required';
      }
      return true;
    },
    shouldRender: (formValues: any) => {
      const index_mode = formValues?.config?.index_mode;
      return index_mode === 'space';
    },
  },
  {
    label: 'Skip hidden pages',
    name: 'config.skip_hidden',
    type: FormFieldType.Checkbox,
    required: false,
    tooltip: t('setting.xwikiSkipHiddenTip'),
  },
];
