import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useAuthStore } from '@/stores/authStore';
import { apiRequest } from '@/lib/api-client';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

interface StickyRow {
  apiKeyID: number;
  keyName: string;
  model: string;
  channelID: number;
  probing: boolean;
  preferredChannelID: number;
  probeChannelID: number;
  candidates: { id: number; name: string }[];
  cooldowns: { channelID: number; failures: number; until: string }[];
  attempted: number[];
}

const queryKey = ['model-sticky-runtime'];

function StickyRouteRow({ row }: { row: StickyRow }) {
  const { t } = useTranslation();
  const client = useQueryClient();
  const [selected, setSelected] = useState('');
  const mutation = useMutation({
    mutationFn: (channelID: number) =>
      apiRequest('/admin/model-sticky', {
        method: 'POST',
        requireAuth: true,
        body: { apiKeyID: row.apiKeyID, model: row.model, channelID },
      }),
    onSuccess: () => {
      setSelected('');
      void client.invalidateQueries({ queryKey });
    },
  });
  const current = row.candidates.find((candidate) => candidate.id === row.channelID);
  const channelLabel = (id: number) => {
    const candidate = row.candidates.find((item) => item.id === id);
    return candidate ? `${candidate.name} (#${id})` : `#${id}`;
  };
  return (
    <div className='space-y-3 rounded-md border p-3'>
      <div className='font-medium break-all'>
        {row.keyName} · Key #{row.apiKeyID} · {row.model}
      </div>
      <p className='text-muted-foreground text-sm'>
        {t('system.retry.modelSticky.current')}:{' '}
        {row.probing
          ? `${t('system.retry.modelSticky.probing')} · ${channelLabel(row.probeChannelID)}`
          : current
            ? `${current.name} (#${current.id})`
            : t('system.retry.modelSticky.unpinned')}
      </p>
      {row.preferredChannelID > 0 && (
        <p className='text-muted-foreground text-sm'>
          {t('system.retry.modelSticky.preferred', { channel: channelLabel(row.preferredChannelID) })}
        </p>
      )}
      <div className='flex flex-wrap items-center gap-2'>
        <Select value={selected} onValueChange={setSelected} disabled={mutation.isPending}>
          <SelectTrigger className='w-72' aria-label={t('system.retry.modelSticky.select')}>
            <SelectValue placeholder={t('system.retry.modelSticky.select')} />
          </SelectTrigger>
          <SelectContent>
            {row.candidates.map((candidate) => (
              <SelectItem key={candidate.id} value={String(candidate.id)}>
                {candidate.name} (#{candidate.id})
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button
          type='button'
          variant='outline'
          disabled={!selected || mutation.isPending}
          onClick={() => mutation.mutate(Number(selected))}
        >
          {t('system.retry.modelSticky.switch')}
        </Button>
        <Button type='button' variant='outline' disabled={mutation.isPending} onClick={() => mutation.mutate(0)}>
          {t('system.retry.modelSticky.reset')}
        </Button>
      </div>
      <p className='text-muted-foreground text-xs'>{t('system.retry.modelSticky.resetHint')}</p>
      {row.cooldowns.length > 0 && (
        <ul className='text-muted-foreground space-y-1 text-sm'>
          {row.cooldowns.map((health) => (
            <li key={health.channelID}>
              #{health.channelID} · {t('system.retry.modelSticky.failures', { count: health.failures })} ·{' '}
              {t('system.retry.modelSticky.until')} {new Date(health.until).toLocaleTimeString()}
            </li>
          ))}
        </ul>
      )}
      {mutation.error && (
        <p role='alert' className='text-destructive text-sm'>
          {mutation.error.message}
        </p>
      )}
    </div>
  );
}

export function ModelStickyControls() {
  const { t } = useTranslation();
  const isOwner = useAuthStore((state) => state.auth.user?.isOwner === true);
  const query = useQuery({
    queryKey,
    queryFn: () => apiRequest<StickyRow[]>('/admin/model-sticky', { requireAuth: true }),
    enabled: isOwner,
    refetchInterval: 5000,
  });
  if (!isOwner) return null;
  return (
    <section className='mt-6 space-y-3 border-t pt-6'>
      <div className='flex items-center justify-between gap-3'>
        <h3 className='font-medium'>{t('system.retry.modelSticky.title')}</h3>
        <Button type='button' variant='outline' disabled={query.isFetching} onClick={() => void query.refetch()}>
          {t('system.retry.modelSticky.refresh')}
        </Button>
      </div>
      <p className='text-muted-foreground text-sm'>{t('system.retry.modelSticky.description')}</p>
      {query.error && (
        <p role='alert' className='text-destructive text-sm'>
          {query.error.message}
        </p>
      )}
      {!query.isPending && !query.error && query.data?.length === 0 && (
        <p className='text-muted-foreground text-sm'>{t('system.retry.modelSticky.empty')}</p>
      )}
      {query.data?.map((row) => (
        <StickyRouteRow key={`${row.apiKeyID}:${row.model}`} row={row} />
      ))}
    </section>
  );
}
